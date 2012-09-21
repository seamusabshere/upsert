class Upsert
  class Cell
    # @private
    class PG_Connection < Cell
      attr_reader :name
      attr_reader :value
      attr_reader :quoted_name

      def initialize(connection, name, value)
        @name = name
        @value = value
        @quoted_name = connection.quote_ident name
      end

      def bind_value
        return @bind_value if defined?(@bind_value)
        @bind_value = case value
        when Upsert::Binary
          { :value => value.value, :format => 1 }
        when Time, DateTime
          [value.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, value.usec)].join('.')
        else
          value
        end
      end
    end
  end
end
