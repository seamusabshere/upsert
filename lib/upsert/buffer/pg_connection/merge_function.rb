require 'digest/md5'

class Upsert
  # @private
  class Buffer
    class PG_Connection < Buffer
      class MergeFunction
        class << self
          def execute(buffer, row)
            merge_function = lookup buffer, row
            merge_function.execute row
          end

          def unique_name(table_name, selector, setter)
            # $stderr.puts "AAA #{table_name}/#{selector}/#{setter}"
            parts = [
              'upsert',
              table_name,
              'SEL',
              selector.join('_A_'),
              'SET',
              setter.join('_A_')
            ].join('_')
            crc32 = Zlib.crc32(parts).to_s
            [ parts.first(MAX_NAME_LENGTH-11), crc32 ].join
          end

          def lookup(buffer, row)
            @lookup ||= {}
            s = row.selector.keys
            c = row.setter.keys
            @lookup[unique_name(buffer.parent.table_name, s, c)] ||= new(buffer, s, c)
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
          @name ||= MergeFunction.unique_name buffer.parent.table_name, selector, setter
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

        def quoted_table_name
          buffer.parent.quoted_table_name
        end

        # [upsert] SELECT upsert_pets_SEL_name_SET_birthday_A_good_A_home_addr2097355686($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)

        class ColumnDefinition
          attr_reader :name, :sql_type, :default, :quoted_name, :quoted_selector_name, :quoted_setter_name
          def initialize(connection, name, sql_type, default)
            @name = name
            @sql_type = sql_type
            @default = default
            @quoted_name = connection.quote_ident name
            @quoted_selector_name = connection.quote_ident "#{name}_selector"
            @quoted_setter_name = connection.quote_ident "#{name}_setter"
          end

          def to_selector_arg
            [ quoted_selector_name, sql_type ].join ' '
            # [ quoted_selector_name, sql_type, 'DEFAULT', (default || 'NULL') ].join ' '
          end

          def to_setter_arg
            [ quoted_setter_name, sql_type ].join ' '
            # [ quoted_setter_name, sql_type, 'DEFAULT', (default || 'NULL') ].join ' '
          end

          def to_setter
            "#{quoted_name} = #{quoted_setter_name}"
          end

          def to_selector
            "#{quoted_name} = #{quoted_selector_name}"
          end
        end

        # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
        def get_column_definitions
          res = connection.execute <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{quoted_table_name}'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
EOS
          res.map do |row|
            ColumnDefinition.new connection, row['name'], row['sql_type'], row['default']
          end.sort_by do |cd|
            cd.name
          end
        end

        # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
        # differentiate between selector and setter
        def create!
          Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{buffer.parent.table_name.inspect} for selector #{selector.map(&:inspect).join(', ')} and setter #{setter.map(&:inspect).join(', ')}"
          column_definitions = get_column_definitions
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
          IF (first_try) THEN
            first_try = 0;
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
