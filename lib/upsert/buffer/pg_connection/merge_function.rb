require 'digest/md5'

class Upsert
  class Buffer
    class PG_Connection < Buffer
      # @private
      class MergeFunction
        class << self
          def execute(buffer, row)
            merge_function = lookup buffer, row
            merge_function.execute row
          end

          def unique_name(table_name, selector, setter)
            parts = [
              'upsert',
              table_name,
              'SEL',
              selector.join('_A_'),
              'SET',
              setter.join('_A_')
            ].join('_')
            # maybe i should md5 instead
            crc32 = Zlib.crc32(parts).to_s
            [ parts.first(MAX_NAME_LENGTH-11), crc32 ].join
          end

          def lookup(buffer, row)
            @lookup ||= {}
            selector = row.selector.keys
            setter = row.setter.keys
            key = [buffer.parent.table_name, selector, setter]
            @lookup[key] ||= new(buffer, selector, setter)
          end

          def clear(buffer)
            connection = buffer.parent.connection
            # http://stackoverflow.com/questions/7622908/postgresql-drop-function-without-knowing-the-number-type-of-parameters
            connection.execute <<-EOS
CREATE OR REPLACE FUNCTION pg_temp.upsert_delfunc(text)
  RETURNS void AS
$BODY$
DECLARE
   _sql text;
BEGIN

FOR _sql IN
   SELECT 'DROP FUNCTION ' || quote_ident(n.nspname)
                    || '.' || quote_ident(p.proname)
           || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ');'
   FROM   pg_catalog.pg_proc p
   LEFT   JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE  p.proname = $1
   AND    pg_catalog.pg_function_is_visible(p.oid) -- you may or may not want this
LOOP
   EXECUTE _sql;
END LOOP;

END;
$BODY$
  LANGUAGE plpgsql;
EOS
            res = connection.execute(%{SELECT proname FROM pg_proc WHERE proname LIKE 'upsert_%'})
            res.each do |row|
              k = row['proname']
              next if k == 'upsert_delfunc'
              Upsert.logger.info %{[upsert] Dropping function #{k.inspect}}
              connection.execute %{SELECT pg_temp.upsert_delfunc('#{k}')}
            end
          end
        end

        MAX_NAME_LENGTH = 63

        attr_reader :buffer
        attr_reader :selector
        attr_reader :setter

        def initialize(buffer, selector, setter)
          @buffer = buffer
          @selector = selector
          @setter = setter
          create!
        end

        def name
          @name ||= MergeFunction.unique_name table_name, selector, setter
        end

        def execute(row)
          first_try = true
          bind_selector_values = row.selector.values.map(&:bind_value)
          bind_setter_values = row.setter.values.map(&:bind_value)
          begin
            connection.execute sql, (bind_selector_values + bind_setter_values)
          rescue PG::Error => pg_error
            if pg_error.message =~ /function #{name}.* does not exist/i
              if first_try
                Upsert.logger.info %{[upsert] Function #{name.inspect} went missing, trying to recreate}
                first_try = false
                create!
                retry
              else
                Upsert.logger.info %{[upsert] Failed to create function #{name.inspect} for some reason}
                raise pg_error
              end
            else
              raise pg_error
            end
          end
        end

        private

        def sql
          @sql ||= begin
            bind_params = []
            1.upto(selector.length + setter.length) { |i| bind_params << "$#{i}" }
            %{SELECT #{name}(#{bind_params.join(', ')})}
          end
        end

        def connection
          buffer.parent.connection
        end

        def table_name
          buffer.parent.table_name
        end

        def quoted_table_name
          buffer.parent.quoted_table_name
        end

        # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
        # differentiate between selector and setter
        def create!
          Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{table_name.inspect} for selector #{selector.map(&:inspect).join(', ')} and setter #{setter.map(&:inspect).join(', ')}"
          column_definitions = ColumnDefinition.all buffer, table_name
          selector_column_definitions = column_definitions.select { |cd| selector.include?(cd.name) }
          setter_column_definitions = column_definitions.select { |cd| setter.include?(cd.name) }
          connection.execute <<-EOS
CREATE OR REPLACE FUNCTION #{name}(#{(selector_column_definitions.map(&:to_selector_arg) + setter_column_definitions.map(&:to_setter_arg)).join(', ')}) RETURNS VOID AS
$$
DECLARE
  first_try INTEGER := 1;
BEGIN
  LOOP
      -- first try to update the key
      UPDATE #{quoted_table_name} SET #{setter_column_definitions.map(&:to_setter).join(', ')}
          WHERE #{selector_column_definitions.map(&:to_selector).join(' AND ') };
      IF found THEN
          RETURN;
      END IF;
      -- not there, so try to insert the key
      -- if someone else inserts the same key concurrently,
      -- we could get a unique-key failure
      BEGIN
          INSERT INTO #{quoted_table_name}(#{setter_column_definitions.map(&:quoted_name).join(', ')}) VALUES (#{setter_column_definitions.map(&:quoted_setter_name).join(', ')});
          RETURN;
      EXCEPTION WHEN unique_violation THEN
          -- seamusabshere 9/20/12 only retry once
          IF (first_try = 1) THEN
            first_try := 0;
          ELSE
            RETURN;
          END IF;
          -- Do nothing, and loop to try the UPDATE again.
      END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;
EOS
        end
      end
    end
  end
end
