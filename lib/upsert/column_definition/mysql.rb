class Upsert
  class ColumnDefinition
    # @private
    class Mysql < ColumnDefinition
      class << self
        def all(connection, quoted_table_name)
          connection.execute("SHOW COLUMNS FROM #{quoted_table_name}").map do |row|
            # {"Field"=>"name", "Type"=>"varchar(255)", "Null"=>"NO", "Key"=>"PRI", "Default"=>nil, "Extra"=>""}
            name = row['Field'] || row['COLUMN_NAME'] || row[:Field] || row[:COLUMN_NAME]
            type = row['Type'] || row['COLUMN_TYPE'] || row[:Type] || row[:COLUMN_TYPE]
            default = row['Default'] || row['COLUMN_DEFAULT'] || row[:Default] || row[:COLUMN_DEFAULT]
            new connection, name, type, default
          end.sort_by do |cd|
            cd.name
          end
        end
      end

      def equality(left, right)
        "#{left} <=> #{right}"
      end
    end
  end
end
