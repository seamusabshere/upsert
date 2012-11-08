class Upsert
  class ColumnDefinition
    # @private
    class Mysql2_Client < ColumnDefinition
      class << self
        def all(connection, table_name)
          connection.execute("SHOW COLUMNS FROM #{connection.quote_ident(table_name)}").map do |row|
            name, type, default = if row.is_a?(Array)
              # you don't know if mysql2 is going to give you an array or a hash... and you shouldn't specify, because it's sticky
              # ["name", "varchar(255)", "YES", "UNI", nil, ""]
              row.values_at(0,1,4)
            else
              # {"Field"=>"name", "Type"=>"varchar(255)", "Null"=>"NO", "Key"=>"PRI", "Default"=>nil, "Extra"=>""}
              [row['Field'], row['Type'], row['Default']]
            end
            new connection, name, type, default
          end.sort_by do |cd|
            cd.name
          end
        end
      end
    end
  end
end
