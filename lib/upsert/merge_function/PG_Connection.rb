require 'upsert/merge_function/postgresql'

class Upsert
  class MergeFunction
    # @private
    class PG_Connection < MergeFunction
      ERROR_CLASS = PG::Error
      include Postgresql

      def execute_parameterized(query, args = [])
        controller.connection.execute(query, args)
      end

      def unique_index_on_selector?
        return @unique_index_on_selector if defined?(@unique_index_on_selector)
        @unique_index_on_selector = begin
          type_map = PG::TypeMapByColumn.new([PG::TextDecoder::Array.new])
          schema_query.type_map = type_map

          schema_query.values.any? do |row|
            row.first.sort == selector_keys.sort
          end
        end
      end
    end
  end
end
