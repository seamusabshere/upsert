class Upsert
  class Cell
    # @private
    class Mysql2_Client < Cell
      attr_reader :name, :value, :quoted_value
      def initialize(connection, name, value)
        @name = name
        @value = value
        @quoted_value = connection.quote_value value
      end
    end
  end
end
