require_relative "jdbc"
require_relative "postgresql"

class Upsert
  class Connection
    # @private
    class Java_OrgPostgresqlJdbc_PgConnection < Connection
      include Jdbc
      include Postgresql

      def quote_ident(k)
        DOUBLE_QUOTE + k.to_s.gsub(DOUBLE_QUOTE, '""') + DOUBLE_QUOTE
      end

      def in_transaction?
        # https://github.com/kares/activerecord-jdbc-adapter/commit/4d6e0e0c52d12b0166810dffc9f898141a23bee6
        ![0, 4].include?(metal.get_transaction_state)
      end

      def bind_value(v)
        case v
        when DateTime, Time
          date = v.utc
          java.time.LocalDateTime.of(date.year, date.month, date.day, date.hour, date.min, date.sec, date.nsec)
        when Date
          java.time.LocalDate.of(v.year, v.month, v.day)
        when Array
          # Make sure it's passed through.  Encoding handled at the JDBC level
          v
        else
          super
        end
      end
    end
  end
end
