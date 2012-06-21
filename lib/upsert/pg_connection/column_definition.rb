class Upsert
  module PG_Connection
    # @private
    # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
    class ColumnDefinition
      class << self
        def auto_increment_primary_key(connection, table_name)
          res = connection.exec <<-EOS
SELECT attr.attname, seq.relname
FROM pg_class      seq,
   pg_attribute  attr,
   pg_depend     dep,
   pg_namespace  name,
   pg_constraint cons
WHERE seq.oid           = dep.objid
AND seq.relkind       = 'S'
AND attr.attrelid     = dep.refobjid
AND attr.attnum       = dep.refobjsubid
AND attr.attrelid     = cons.conrelid
AND attr.attnum       = cons.conkey[1]
AND cons.contype      = 'p'
AND dep.refobjid      = '#{connection.quote_ident(table_name.to_s)}'::regclass
EOS
          if hit = res.first
            hit['attname']
          end
        end

        def all(connection, table_name)
          auto_increment_primary_key = auto_increment_primary_key(connection, table_name)
          res = connection.exec <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{connection.quote_ident(table_name.to_s)}'::regclass
 AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
EOS
          res.reject do |row|
            row['name'] == auto_increment_primary_key
          end.map do |row|
            new row['name'], row['sql_type'], row['default']
          end
        end
      end

      attr_reader :name
      attr_reader :input_name
      attr_reader :sql_type
      attr_reader :default
      
      def initialize(name, sql_type, default)
        @name = name
        @input_name = "#{name}_input"
        @sql_type = sql_type
        @default = default
      end
    end
  end
end
