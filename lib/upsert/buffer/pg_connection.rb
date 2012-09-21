require 'upsert/buffer/pg_connection/column_definition'
require 'upsert/buffer/pg_connection/merge_function'

class Upsert
  class Buffer
    # @private
    class PG_Connection < Buffer
      def ready
        return if rows.empty?
        row = rows.shift
        MergeFunction.execute self, row
      end

      def clear_database_functions
        MergeFunction.clear self
      end
    end
  end
end
