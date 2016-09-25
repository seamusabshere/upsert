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

      attr_reader :quoted_setter_names
      attr_reader :quoted_selector_names

      def initialize(controller, *args)
        super
        @quoted_setter_names = setter_keys.map { |k| connection.quote_ident k }
        @quoted_selector_names = selector_keys.map { |k| connection.quote_ident k }
      end

      def execute(row)
        use_pg_native? ? pg_native(row) : pg_function(row)
      end

      def pg_function(row)
        first_try = true
        values = []
        values += row.selector.values
        values += row.setter.values
        hstore_delete_handlers.each do |hstore_delete_handler|
          values << row.hstore_delete_keys.fetch(hstore_delete_handler.name, [])
        end
        Upsert.logger.debug do
          %{[upsert]\n\tSelector: #{row.selector.inspect}\n\tSetter: #{row.setter.inspect}}
        end
        begin
          execute_parameterized(sql, values.map { |v| connection.bind_value v })
        rescue self.class::ERROR_CLASS => pg_error
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

      # strangely ? can't be used as a placeholder
      def sql
        @sql ||= begin
          bind_params = []
          i = 1
          (selector_keys.length + setter_keys.length).times do
            bind_params << "$#{i}"
            i += 1
          end
          hstore_delete_handlers.length.times do
            bind_params << "$#{i}::text[]"
            i += 1
          end
          %{SELECT #{name}(#{bind_params.join(', ')})}
        end
      end

      def use_pg_native?
        server_version >= 95 && unique_index_on_selector?
      end

      def server_version
        @server_version ||=
          controller.connection.execute("SHOW server_version").first["server_version"].split('.')[0..1].join('').to_i
      end

      def schema_query
        execute_parameterized(
          %{
            SELECT array_agg(column_name::text) AS index_columns FROM information_schema.constraint_column_usage
              JOIN pg_catalog.pg_constraint ON constraint_name::text = conname::text
              WHERE table_name = $1 AND conrelid = $1::regclass::oid AND contype = 'u'
              GROUP BY table_catalog, table_name, constraint_name
          },
          [table_name]
        )
      end

      def pg_native(row)
        bind_setter_values = row.setter.values.map { |v| connection.bind_value v }

        upsert_sql = %{
          INSERT INTO #{quoted_table_name} (#{quoted_setter_names.join(',')})
          VALUES (#{insert_bind_placeholders(row).join(', ')})
          ON CONFLICT(#{quoted_selector_names.join(', ')})
          DO UPDATE SET (#{quoted_setter_names.join(', ')}) = (#{conflict_bind_placeholders(row).join(', ')})
        }

        execute_parameterized(upsert_sql, bind_setter_values)
      end

      def hstore_delete_function(sql, row, column_definition)
        parts = []
        if row.hstore_delete_keys.key?(column_definition.name)
          parts << "DELETE("
        end
        parts << sql
        if row.hstore_delete_keys.key?(column_definition.name)
          keys = row.hstore_delete_keys[column_definition.name].map { |k| "'#{k.to_s.gsub("'", "\\'")}'" }
          parts << ", ARRAY[#{keys.join(', ')}])"
        end

        parts.join(" ")
      end

      def insert_bind_placeholders(row)
        if row.hstore_delete_keys.empty?
          @insert_bind_placeholders ||= setter_column_definitions.each_with_index.map do |column_definition, i|
            "$#{i + 1}"
          end
        else
          setter_column_definitions.each_with_index.map do |column_definition, i|
            idx = i + 1
            if column_definition.hstore?
              hstore_delete_function("$#{idx}", row, column_definition)
            else
              "$#{idx}"
            end
          end
        end
      end

      def conflict_bind_placeholders(row)
        if row.hstore_delete_keys.empty?
          @conflict_bind_placeholders ||= setter_column_definitions.each_with_index.map do |column_definition, i|
            idx = i + 1
            if column_definition.hstore?
              "CASE WHEN #{quoted_table_name}.#{column_definition.quoted_name} IS NULL THEN $#{idx} ELSE" \
                + " (#{quoted_table_name}.#{column_definition.quoted_name} || $#{idx})" \
                + " END"
            else
              "$#{idx}"
            end
          end
        else
          setter_column_definitions.each_with_index.map do |column_definition, i|
            idx = i + 1
            if column_definition.hstore?
              "CASE WHEN #{quoted_table_name}.#{column_definition.quoted_name} IS NULL THEN " \
                + hstore_delete_function("$#{idx}", row, column_definition) \
                + " ELSE " \
                + hstore_delete_function("(#{quoted_table_name}.#{column_definition.quoted_name} || $#{idx})", row, column_definition) \
                + " END"
            else
              "$#{idx}"
            end
          end
        end
      end

      class HstoreDeleteHandler
        attr_reader :merge_function
        attr_reader :column_definition
        def initialize(merge_function, column_definition)
          @merge_function = merge_function
          @column_definition = column_definition
        end
        def name
          column_definition.name
        end
        def to_arg
          "#{quoted_name} text[]"
        end
        # use coalesce(foo, '{}':text[])
        def to_setter
          "#{column_definition.quoted_name} = DELETE(#{column_definition.quoted_name}, #{quoted_name})"
        end
        def to_pgsql
          %{
            IF array_length(#{quoted_name}, 1) > 0 THEN
              UPDATE #{merge_function.quoted_table_name} SET #{to_setter}
                WHERE #{merge_function.selector_column_definitions.map(&:to_selector).join(' AND ') };
            END IF;
          }.gsub(/\s+/, ' ')
        end
        private
        def quoted_name
          @quoted_name ||= merge_function.connection.quote_ident "_delete_#{column_definition.name}"
        end
      end

      def hstore_delete_handlers
        @hstore_delete_handlers ||= setter_column_definitions.select do |column_definition|
          column_definition.hstore?
        end.map do |column_definition|
          HstoreDeleteHandler.new self, column_definition
        end
      end

      def selector_column_definitions
        column_definitions.select { |cd| selector_keys.include?(cd.name) }
      end

      def setter_column_definitions
        column_definitions.select { |cd| setter_keys.include?(cd.name) }
      end

      def update_column_definitions
        setter_column_definitions.select { |cd| cd.name !~ CREATED_COL_REGEX }
      end

      # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
      # differentiate between selector and setter
      def create!
        Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{table_name.inspect} for selector #{selector_keys.map(&:inspect).join(', ')} and setter #{setter_keys.map(&:inspect).join(', ')}"
        first_try = true
        connection.execute(%{
          CREATE OR REPLACE FUNCTION #{name}(#{(selector_column_definitions.map(&:to_selector_arg) + setter_column_definitions.map(&:to_setter_arg) + hstore_delete_handlers.map(&:to_arg)).join(', ')}) RETURNS VOID AS
          $$
          DECLARE
            first_try INTEGER := 1;
          BEGIN
            LOOP
              -- first try to update the key
              UPDATE #{quoted_table_name} SET #{update_column_definitions.map(&:to_setter).join(', ')}
                WHERE #{selector_column_definitions.map(&:to_selector).join(' AND ') };
              IF found THEN
                #{hstore_delete_handlers.map(&:to_pgsql).join(' ')}
                RETURN;
              END IF;
              -- not there, so try to insert the key
              -- if someone else inserts the same key concurrently,
              -- we could get a unique-key failure
              BEGIN
                INSERT INTO #{quoted_table_name}(#{setter_column_definitions.map(&:quoted_name).join(', ')}) VALUES (#{setter_column_definitions.map(&:to_setter_value).join(', ')});
                #{hstore_delete_handlers.map(&:to_pgsql).join(' ')}
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
