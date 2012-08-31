class Upsert
  class Connection
    # @private
    class SQLite3_Database < Connection
      def execute(sql)
        Upsert.logger.debug { %{[upsert] #{sql}} }
        raw_connection.execute_batch sql
      end

      def quote_string(v)
        SINGLE_QUOTE + SQLite3::Database.quote(v) + SINGLE_QUOTE
      end

      def quote_binary(v)
        X_AND_SINGLE_QUOTE + v.unpack("H*")[0] + SINGLE_QUOTE
      end

      def quote_time(v)
        quote_string [v.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, v.usec)].join('.')
      end
      
      def quote_ident(k)
        DOUBLE_QUOTE + SQLite3::Database.quote(k.to_s) + DOUBLE_QUOTE
      end

      def quote_boolean(v)
        s = v ? 't' : 'f'
        quote_string s
      end

      def quote_big_decimal(v)
        v.to_f
      end
    end
  end
end
