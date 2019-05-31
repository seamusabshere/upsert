class Upsert
  class ColumnDefinition
    # @private
    class Mysql < ColumnDefinition
      class << self
        def all(connection, table_name)
          connection.execute("SHOW COLUMNS FROM #{connection.quote_ident(table_name)}").map { |row|
            # {"Field"=>"name", "Type"=>"varchar(255)", "Null"=>"NO", "Key"=>"PRI", "Default"=>nil, "Extra"=>""}
            name = row["Field"] || row["COLUMN_NAME"] || row[:Field] || row[:COLUMN_NAME]
            type = row["Type"] || row["COLUMN_TYPE"] || row[:Type] || row[:COLUMN_TYPE]
            default = row["Default"] || row["COLUMN_DEFAULT"] || row[:Default] || row[:COLUMN_DEFAULT]
            new connection, name, type, default
          }.sort_by do |cd|
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
