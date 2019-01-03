require 'upsert/connection/postgresql'

class Upsert
  class Connection
    # @private
    class PG_Connection < Connection
      include Postgresql

      def execute(sql, params = nil)
        if params
          # Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          # The following will blow up if you pass a value that cannot be automatically type-casted,
          #   such as passing a string to an integer field.  You'll get an error something along the
          #   lines of: "invalid input syntax for <type>: <value>"
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
