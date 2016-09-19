require 'upsert/merge_function/postgresql'

class Upsert
  class MergeFunction
    # @private
    class PG_Connection < MergeFunction
      include Postgresql

      attr_reader :quoted_setter_names
      attr_reader :quoted_selector_names

      def self.use_pg_upsert_cache
        @use_pg_upsert_cache ||= {}
      end

      def initialize(controller, *args)
        super
        @quoted_setter_names = setter_keys.map { |k| connection.quote_ident k }
        @quoted_selector_names = selector_keys.map { |k| connection.quote_ident k }
      end

      def execute(row)
        use_pg_upsert? ? pg_upsert(row) : pg_function(row)
      end

      def use_pg_upsert?
        return if @use_native_upsert == false
        # We need to memoize this way because we're storing a boolean
        return self.class.use_pg_upsert_cache[__id__] if self.class.use_pg_upsert_cache.key?(__id__)

        self.class.use_pg_upsert_cache[__id__] = begin
          version = controller.connection.execute("SHOW server_version").getvalue(0, 0)
          version_number = version[0..2].split('.').join('').to_i

          matching_constraint = false
          schema_query = controller.connection.execute("SELECT array_agg(column_name::text) FROM information_schema.constraint_column_usage WHERE table_name = $1 GROUP BY table_catalog, table_name, constraint_name", [table_name])
          type_map = PG::TypeMapByColumn.new([PG::TextDecoder::Array.new])
          schema_query.type_map = type_map

          schema_query.values.each do |row|
            if row.first.sort == selector_keys.sort
              matching_constraint = true
              break
            end
          end

          version_number >= 95 && matching_constraint
        end
      end

      def pg_upsert(row)
        bind_setter_values = row.setter.values.map { |v| connection.bind_value v }
        bind_selector_values = row.selector.values.map { |v| connection.bind_value v }

        upsert_sql = %{
          INSERT INTO #{quoted_table_name} (#{quoted_setter_names.join(',')})
          VALUES (#{bind_placeholders.join(',')})
          ON CONFLICT(#{quoted_selector_names.join(', ')})
          DO UPDATE SET (#{quoted_setter_names.join(', ')}) = (#{bind_placeholders.join(',')})
        }
        connection.execute upsert_sql, bind_setter_values
      end

      def bind_placeholders
        @bind_placeholders ||= begin
          setter_keys.each_with_index().each_with_object([]) do |(v, i), memo|
            memo << "$#{i + 1}"
          end
        end
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
          connection.execute sql, values.map { |v| connection.bind_value v }
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
    end
  end
end
