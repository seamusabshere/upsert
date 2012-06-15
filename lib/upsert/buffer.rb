class Upsert
  class Buffer
    class << self
      def for(connection, table_name)
        const_get(connection.class.name.gsub(/\W+/, '_')).new connection, table_name
      end
    end

    SINGLE_QUOTE = %{'}
    DOUBLE_QUOTE = %{"}
    BACKTICK = %{`}
    X_AND_SINGLE_QUOTE = %{x'}
    USEC_SPRINTF = '%06d'
    ISO8601_DATETIME = '%Y-%m-%d %H:%M:%S' #FIXME ignores timezones i think

    attr_reader :connection
    attr_reader :table_name
    attr_reader :rows
    attr_writer :async
    
    def initialize(connection, table_name)
      @connection = connection
      @table_name = table_name
      @rows = []
    end

    def async?
      !!@async
    end

    def add(selector, document)
      rows << Row.new(self, selector, document)
      if sql = chunk
        execute sql
      end
    end

    def clear
      while sql = chunk
        execute sql
      end
    end
  end
end
