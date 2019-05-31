require "upsert/connection/sqlite3"

class Upsert
  class Connection
    # @private
    class SQLite3_Database < Connection
      include Sqlite3

      def execute(sql, params = nil)
        if params
          Upsert.logger.debug { %([upsert] #{sql} with #{params.inspect}) }
          metal.execute sql, convert_binary(params)
        else
          Upsert.logger.debug { %([upsert] #{sql}) }
          metal.execute sql
        end
      end

      def quote_ident(k)
        DOUBLE_QUOTE + SQLite3::Database.quote(k.to_s) + DOUBLE_QUOTE
      end

      def binary(v)
        SQLite3::Blob.new v.value
      end
    end
  end
end
