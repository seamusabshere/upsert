class Upsert
  module PG_Connection
    # @private
    # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
    class ColumnDefinition
      class << self
        def all(connection, table_name)
          res = connection.exec <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{connection.quote_ident(table_name.to_s)}'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
EOS
          res.map do |row|
            new connection, row['name'], row['sql_type'], row['default']
          end
        end
      end

      attr_reader :name
      attr_reader :input_name
      attr_reader :sql_type
      attr_reader :default
      
      def initialize(connection, raw_name, sql_type, default)
        @name = connection.quote_ident raw_name
        @input_name = connection.quote_ident "#{raw_name}_input"
        @sql_type = sql_type
        @default = default
      end
    end
  end
end
