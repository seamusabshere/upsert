class Upsert
  class MergeFunction
    # @private
    module Postgresql
      def self.included(klass)
        klass.extend ClassMethods
      end

      module ClassMethods
        def clear(connection)
          # http://stackoverflow.com/questions/7622908/postgresql-drop-function-without-knowing-the-number-type-of-parameters
          connection.execute(%{
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
          })
          connection.execute(%{SELECT proname FROM pg_proc WHERE proname LIKE '#{MergeFunction::NAME_PREFIX}%'}).each do |row|
            k = row['proname']
            next if k == 'upsert_delfunc'
            Upsert.logger.info %{[upsert] Dropping function #{k.inspect}}
            connection.execute %{SELECT pg_temp.upsert_delfunc('#{k}')}
          end
        end
      end

      def sql
        @sql ||= begin
          bind_params = Array.new(selector_keys.length + setter_keys.length, '?')
          %{SELECT #{name}(#{bind_params.join(', ')})}
        end
      end

      # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
      # differentiate between selector and setter
      def create!
        Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{table_name.inspect} for selector #{selector_keys.map(&:inspect).join(', ')} and setter #{setter_keys.map(&:inspect).join(', ')}"
        selector_column_definitions = column_definitions.select { |cd| selector_keys.include?(cd.name) }
        setter_column_definitions = column_definitions.select { |cd| setter_keys.include?(cd.name) }
        update_column_definitions = setter_column_definitions.select { |cd| cd.name !~ CREATED_COL_REGEX }
        first_try = true
        connection.execute(%{
          CREATE OR REPLACE FUNCTION #{name}(#{(selector_column_definitions.map(&:to_selector_arg) + setter_column_definitions.map(&:to_setter_arg)).join(', ')}) RETURNS VOID AS
          $$
          DECLARE
            first_try INTEGER := 1;
          BEGIN
            LOOP
              -- first try to update the key
              UPDATE #{quoted_table_name} SET #{update_column_definitions.map(&:to_setter).join(', ')}
                WHERE #{selector_column_definitions.map(&:to_selector).join(' AND ') };
              IF found THEN
                RETURN;
              END IF;
              -- not there, so try to insert the key
              -- if someone else inserts the same key concurrently,
              -- we could get a unique-key failure
              BEGIN
                INSERT INTO #{quoted_table_name}(#{setter_column_definitions.map(&:quoted_name).join(', ')}) VALUES (#{setter_column_definitions.map(&:to_setter_value).join(', ')});
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
        })
      rescue
        if first_try and $!.message =~ /tuple concurrently updated/
          first_try = false
          retry
        else
          raise $!
        end
      end
    end
  end
end
