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
        else
          super
        end
      end
    end
  end
end
