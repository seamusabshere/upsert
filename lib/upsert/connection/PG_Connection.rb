class Upsert
  class Connection
    # @private
    class PG_Connection < Connection
      include Postgresql

      def execute(sql, params = nil)
        if params
          # Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          metal.exec sql, convert_binary(params)
        else
          Upsert.logger.debug { %{[upsert] #{sql}} }
          metal.exec sql
        end
      end

      def quote_ident(k)
        metal.quote_ident k.to_s
      end

      def binary(v)
        { :value => v.value, :format => 1 }
      end

      def in_transaction?
        ![PG::PQTRANS_IDLE, PG::PQTRANS_UNKNOWN].include?(metal.transaction_status)
      end
    end
  end
end
