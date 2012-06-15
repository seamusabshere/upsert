class Upsert
  class Buffer
    class SQLite3_Database < Buffer
      QUOTE_VALUE = SINGLE_QUOTE
      QUOTE_IDENT = DOUBLE_QUOTE
      USEC_PRECISION = true

      include Quoter

      def chunk
        return false if rows.empty?
        row = rows.shift
        %{
          INSERT OR IGNORE INTO "#{table_name}" (#{row.columns_sql}) VALUES (#{row.values_sql});
          UPDATE "#{table_name}" SET #{row.set_sql} WHERE #{row.where_sql}
        }
      end

      def execute(sql)
        connection.execute_batch sql
      end

      def escape_string(v)
        SQLite3::Database.quote v
      end

      def escape_ident(k)
        k
      end
    end
  end
end
