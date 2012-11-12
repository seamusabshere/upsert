class Upsert
  class MergeFunction
    # @private
    class PG_Connection < MergeFunction
      MAX_NAME_LENGTH = 63

      class << self
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
          connection.execute(%{SELECT proname FROM pg_proc WHERE proname LIKE 'upsert_%'}).each do |row|
            k = row['proname']
            next if k == 'upsert_delfunc'
            Upsert.logger.info %{[upsert] Dropping function #{k.inspect}}
            connection.execute %{SELECT pg_temp.upsert_delfunc('#{k}')}
          end
        end
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

      def sql
        @sql ||= begin
          bind_params = []
          1.upto(selector_keys.length + setter_keys.length) { |i| bind_params << "$#{i}" }
          %{SELECT #{name}(#{bind_params.join(', ')})}
        end
      end

      # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
      # differentiate between selector and setter
      def create!
        Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{table_name.inspect} for selector #{selector_keys.map(&:inspect).join(', ')} and setter #{setter_keys.map(&:inspect).join(', ')}"
        selector_column_definitions = column_definitions.select { |cd| selector_keys.include?(cd.name) }
        setter_column_definitions = column_definitions.select { |cd| setter_keys.include?(cd.name) }
        connection.execute(%{
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
        })
      end
    end
  end
end
