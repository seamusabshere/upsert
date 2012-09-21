class Upsert
  class Connection
    # @private
    class PG_Connection < Connection
      def execute(sql, params = nil)
        if params
          Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          raw_connection.exec sql, params
        else
          Upsert.logger.debug { %{[upsert] #{sql}} }
          raw_connection.exec sql
        end
      end

      def quote_ident(k)
        raw_connection.quote_ident k.to_s
      end
    end
  end
end
