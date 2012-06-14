class Upsert
  class Buffer
    class << self
      def for(connection, table_name)
        const_get(connection.class.name.gsub(/\W+/, '_')).new connection, table_name
      end
    end

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
      rows << Row.new(selector, document)
      if sql = chunk
        execute sql
      end
    end

    def clear
      while sql = chunk(true)
        execute sql
      end
    end

    def chunk(allow_undersized = false)
      return if rows.empty?
      targets = []
      sql = nil
      begin
        targets << rows.pop
        last_sql = sql
        sql = compose(targets)
      end until rows.empty? or targets.length == max_targets or sql.length > max_length
      if sql.length > max_length
        raise if last_sql.nil?
        sql = last_sql
        rows << targets.pop
      elsif async? and not allow_undersized
        @rows += targets
        nil
      else
        sql
      end
    end
  end
end
