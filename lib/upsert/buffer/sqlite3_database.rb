class Upsert
  class Buffer
    class SQLite3_Database < Buffer
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

      def quote_string(v)
        SINGLE_QUOTE + SQLite3::Database.quote(v) + SINGLE_QUOTE
      end

      def quote_binary(v)
        raise "binary not supported for sqlite upsert currently"
      end

      def quote_time(v)
        quote_string [v.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, v.usec)].join('.')
      end
      
      def quote_ident(k)
        DOUBLE_QUOTE + SQLite3::Database.quote(k.to_s) + DOUBLE_QUOTE
      end
    end
  end
end
