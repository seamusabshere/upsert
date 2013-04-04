class Upsert
  class ColumnDefinition
    # @private
    class Sqlite3 < ColumnDefinition
      class << self
        def all(connection, table_name)
          # activerecord-3.2.13/lib/active_record/connection_adapters/sqlite_adapter.rb
          connection.execute("PRAGMA table_info(#{connection.quote_ident(table_name)})").map do |row|#, 'SCHEMA').to_hash
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
    end
  end
end
