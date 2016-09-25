require 'upsert/merge_function/postgresql'

class Upsert
  class MergeFunction
    # @private
    class Java_OrgPostgresqlJdbc4_Jdbc4Connection < MergeFunction
      ERROR_CLASS = org.postgresql.util.PSQLException
      include Postgresql

      def execute_parameterized(query, args = [])
        query_args = []
        query = query.gsub(/\$(\d+)/) do |str|
          query_args << args[$1.to_i - 1]
          "?"
        end

        controller.connection.execute(query, query_args)
      end

      def unique_index_on_selector?
        return @unique_index_on_selector if defined?(@unique_index_on_selector)
        @unique_index_on_selector = begin
          schema_query.any? do |row|
            row["index_columns"].sort == selector_keys.sort
          end
        end
      end
    end
  end
end
