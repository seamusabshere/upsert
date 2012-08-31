class Upsert
  class Connection
    # @private
    class PG_Connection < Connection
      def execute(sql)
        Upsert.logger.debug { %{[upsert] #{sql}} }
        raw_connection.exec sql
      end

      def quote_string(v)
        SINGLE_QUOTE + raw_connection.escape_string(v) + SINGLE_QUOTE
      end

      def quote_binary(v)
        E_AND_SINGLE_QUOTE + raw_connection.escape_bytea(v) + SINGLE_QUOTE
      end

      def quote_time(v)
        quote_string [v.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, v.usec)].join('.')
      end

      def quote_big_decimal(v)
        v.to_s('F')
      end

      def quote_boolean(v)
        v ? 'TRUE' : 'FALSE'
      end

      def quote_ident(k)
        raw_connection.quote_ident k.to_s
      end
    end

    # @private
    # backwards compatibility - https://github.com/seamusabshere/upsert/issues/2
    PGconn = PG_Connection
  end
end
