class Upsert
  class Connection
    # @private
    module Sqlite3
      def bind_value(v)
        case v
        when BigDecimal
          v.to_s('F')
        when TrueClass
          't'
        when FalseClass
          'f'
        when Time, DateTime
          Upsert.utc_iso8601 v
        when Date
          v.strftime ISO8601_DATE
        else
          v
        end
      end
    end
  end
end
