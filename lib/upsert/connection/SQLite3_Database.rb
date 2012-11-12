class Upsert
  class Connection
    # @private
    class SQLite3_Database < Connection
      def execute(sql, params = nil)
        if params
          Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          raw_connection.execute sql, params
        else
          Upsert.logger.debug { %{[upsert] #{sql}} }
          raw_connection.execute sql
        end
      end
      
      def quote_ident(k)
        DOUBLE_QUOTE + SQLite3::Database.quote(k.to_s) + DOUBLE_QUOTE
      end
    end
  end
end
