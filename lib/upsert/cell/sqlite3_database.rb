class Upsert
  class Cell
    # @private
    class SQLite3_Database < Cell
      attr_reader :name, :value, :quoted_name
      def initialize(connection, name, value)
        @name = name
        @value = value
        @quoted_name = connection.quote_ident name
      end
      def bind_value
        return @bind_value if defined?(@bind_value)
        @bind_value = case value
        when Upsert::Binary
          SQLite3::Blob.new value.value
        when BigDecimal
          value.to_s('F')
        when TrueClass
          't'
        when FalseClass
          'f'
        when Time, DateTime
          [value.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, value.usec)].join('.')
        when Date
          value.strftime ISO8601_DATE
        else
          value
        end
      end
    end
  end
end
