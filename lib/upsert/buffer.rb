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

    def chunk
      return false if rows.empty?
      take = rows.length
      until take == 1 or fits_in_single_query?(take)
        take -= 1
      end
      if async? and not maximal?(take)
        return false
      end
      sql = sql take
      @rows = rows.drop(take)
      sql
    end
  end
end
