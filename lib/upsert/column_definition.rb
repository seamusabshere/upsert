class Upsert
  # @private
  class ColumnDefinition
    class << self
      # activerecord-3.2.X/lib/active_record/connection_adapters/XXXXXXXXX_adapter.rb#column_definitions
      def all(connection, table_name)
        raise "not impl"
      end
    end

    TIME_DETECTOR = /date|time/i

    attr_reader :name
    attr_reader :sql_type
    attr_reader :default
    attr_reader :quoted_name
    attr_reader :quoted_selector_name
    attr_reader :quoted_setter_name

    def initialize(connection, name, sql_type, default)
      @name = name
      @sql_type = sql_type
      @temporal_query = !!(sql_type =~ TIME_DETECTOR)
      @default = default
      @quoted_name = connection.quote_ident name
      @quoted_selector_name = connection.quote_ident "#{name}_sel"
      @quoted_setter_name = connection.quote_ident "#{name}_set"
    end

    def to_selector_arg
      "#{quoted_selector_name} #{arg_type}"
    end

    def to_setter_arg
      "#{quoted_setter_name} #{arg_type}"
    end

    def to_setter
      "#{quoted_name} = #{to_setter_value}"
    end

    def to_selector
      equality(quoted_name, to_selector_value)
    end

    def temporal?
      @temporal_query
    end

    def equality(left, right)
      "(#{left} = #{right} OR (#{left} IS NULL AND #{right} IS NULL))"
    end

    def arg_type
      if temporal?
        'character varying(255)'
      else
        sql_type
      end
    end

    def to_setter_value
      if temporal?
        "CAST(#{quoted_setter_name} AS #{sql_type})"
      else
        quoted_setter_name
      end
    end

    def to_selector_value
      if temporal?
        "CAST(#{quoted_selector_name} AS #{sql_type})"
      else
        quoted_selector_name
      end
    end

  end
end
