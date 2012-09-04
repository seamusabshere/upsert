class Upsert
  class Connection
    # @private
    class Mysql2_Client < Connection
      def execute(sql)
        Upsert.logger.debug { %{[upsert] #{sql}} }
        raw_connection.query sql
      end

      def quote_boolean(v)
        v ? 'TRUE' : 'FALSE'
      end

      def quote_string(v)
        SINGLE_QUOTE + raw_connection.escape(v) + SINGLE_QUOTE
      end

      # This doubles the size of the representation.
      def quote_binary(v)
        X_AND_SINGLE_QUOTE + v.unpack("H*")[0] + SINGLE_QUOTE
      end

      # put raw binary straight into sql
      # might work if we could get the encoding issues fixed when joining together the values for the sql
      # alias_method :quote_binary, :quote_string

      def quote_time(v)
        quote_string v.strftime(ISO8601_DATETIME)
      end

      def quote_ident(k)
        BACKTICK + raw_connection.escape(k.to_s) + BACKTICK
      end

      def quote_big_decimal(v)
        v.to_s('F')
      end

      def database_variable_get(k)
        sql = "SHOW VARIABLES LIKE '#{k}'"
        row = execute(sql).first
        case row
        when Array
          row[1]
        when Hash
          row['Value']
        else
          raise "Don't know what to do if connection.query returns a #{row.class}"
        end
      end
    end
  end
end
