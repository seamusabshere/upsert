class Upsert
  class Buffer
    class PG_Connection < Buffer
      class ColumnDefinition
        class << self
          def all(connection, table_name)
            # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
            res = connection.exec <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
  FROM pg_attribute a LEFT JOIN pg_attrdef d
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
 WHERE a.attrelid = '#{connection.quote_ident(table_name)}'::regclass
   AND a.attnum > 0 AND NOT a.attisdropped
 ORDER BY a.attnum
 EOS
            res.map do |row|
              new row['name'], row['sql_type'], row['default']
            end
          end
        end

        attr_reader :name
        attr_reader :sql_type
        attr_reader :default
        
        def initialize(name, sql_type, default)
          @name = name
          @sql_type = sql_type
          @default = default
        end
      end
    end
  end
end
