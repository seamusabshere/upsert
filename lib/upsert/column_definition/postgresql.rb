class Upsert
  class ColumnDefinition
    # @private
    class Postgresql < ColumnDefinition
      class << self
        # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
        def all(connection, table_name)
          res = connection.execute <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{connection.quote_ident(table_name.gsub('"', '').split('.'))}'::regclass
AND a.attnum > 0 AND NOT a.attisdropped
EOS
          res.map do |row|
            new connection, row['name'], row['sql_type'], row['default']
          end.sort_by do |cd|
            cd.name
          end
        end
      end

      # NOTE not using this because it can't be indexed
      # def equality(left, right)
      #   "#{left} IS NOT DISTINCT FROM #{right}"
      # end

      HSTORE_DETECTOR = /hstore/i

      def initialize(*)
        super
        @hstore_query = !!(sql_type =~ HSTORE_DETECTOR)
      end

      def hstore?
        @hstore_query
      end

      def arg_type
        if hstore?
          'text'
        else
          super
        end
      end

      def to_setter_value
        if hstore?
          "#{quoted_setter_name}::hstore"
        else
          super
        end
      end

      def to_setter
        if hstore?
          # http://stackoverflow.com/questions/9317971/adding-a-key-to-an-empty-hstore-column
          "#{quoted_name} = COALESCE(#{quoted_name}, hstore(array[]::varchar[])) || #{to_setter_value}"
        else
          super
        end
      end
    end
  end
end
