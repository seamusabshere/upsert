class Upsert
  class ColumnDefinition
    # @private
    class Sqlite3 < ColumnDefinition
      class << self
        def all(connection, quoted_table_name)
          # activerecord-3.2.13/lib/active_record/connection_adapters/sqlite_adapter.rb
          connection.execute("PRAGMA table_info(#{quoted_table_name})").map do |row|#, 'SCHEMA').to_hash
            if connection.metal.respond_to?(:results_as_hash) and not connection.metal.results_as_hash
              row = {'name' => row[1], 'type' => row[2], 'dflt_value' => row[4]}
            end
            default = case row["dflt_value"]
            when /^null$/i
              nil
            when /^'(.*)'$/
              $1.gsub(/''/, "'")
            when /^"(.*)"$/
              $1.gsub(/""/, '"')
            else
              row["dflt_value"]
            end
            new connection, row['name'], row['type'], default
          end.sort_by do |cd|
            cd.name
          end
        end
      end

      def equality(left, right)
        "(#{left} IS #{right} OR (#{left} IS NULL AND #{right} IS NULL))"
      end
    end
  end
end
