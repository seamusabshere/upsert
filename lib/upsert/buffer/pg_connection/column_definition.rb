class Upsert
  class Buffer
    class PG_Connection < Buffer
      # @private
      class ColumnDefinition
        class << self
          # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
          def all(buffer, table_name)
            connection = buffer.parent.connection
            res = connection.execute <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{connection.quote_ident(table_name)}'::regclass
AND a.attnum > 0 AND NOT a.attisdropped
EOS
            res.map do |row|
              new connection, row['name'], row['sql_type'], row['default']
            end.sort_by do |cd|
              cd.name
            end
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
          @quoted_selector_name = connection.quote_ident "#{name}_selector"
          @quoted_setter_name = connection.quote_ident "#{name}_setter"
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
  end
end
