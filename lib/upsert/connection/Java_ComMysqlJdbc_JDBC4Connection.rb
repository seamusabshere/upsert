require 'upsert/connection/jdbc'

class Upsert
  class Connection
    # @private
    class Java_ComMysqlJdbc_JDBC4Connection < Connection
      include Jdbc

      def quote_ident(k)
        if metal.useAnsiQuotedIdentifiers
          DOUBLE_QUOTE + k.to_s.gsub(DOUBLE_QUOTE, '""') + DOUBLE_QUOTE
        else
          # Escape backticks by doubling them.  Ref http://dev.mysql.com/doc/refman/5.7/en/identifiers.html
          BACKTICK + k.to_s.gsub(BACKTICK, BACKTICK + BACKTICK) + BACKTICK
        end
      end

      def bind_value(v)
        case v
        when Time, DateTime
          # mysql doesn't like it when you send timezone to a datetime
          Upsert.utc_iso8601 v, false
        else
          super
        end
      end
    end
  end
end
