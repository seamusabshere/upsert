class Upsert
  # @private
  class ColumnDefinition
    class << self
      # activerecord-3.2.X/lib/active_record/connection_adapters/XXXXXXXXX_adapter.rb#column_definitions
      def all(connection, table_name)
        raise "not impl"
      end
    end

    attr_reader :name
    attr_reader :sql_type
    attr_reader :default
    attr_reader :quoted_name
    attr_reader :quoted_selector_name
    attr_reader :quoted_setter_name

    def initialize(connection, name, sql_type, default)
      @name = name
      @sql_type = sql_type
      @default = default
      @quoted_name = connection.quote_ident name
      @quoted_selector_name = connection.quote_ident "#{name}_sel"
      @quoted_setter_name = connection.quote_ident "#{name}_set"
    end

    def to_selector_arg
      "#{quoted_selector_name} #{sql_type}"
    end

    def to_setter_arg
      "#{quoted_setter_name} #{sql_type}"
    end

    def to_setter
      "#{quoted_name} = #{quoted_setter_name}"
    end

    def to_selector
      "#{quoted_name} = #{quoted_selector_name}"
    end
  end
end
