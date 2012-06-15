class Upsert
  class Buffer
    class SQLite3_Database < Buffer
      MAX_CONCURRENCY = 1
      QUOTE_VALUE = SINGLE_QUOTE
      QUOTE_IDENT = DOUBLE_QUOTE

      include Quoter

        # parts = []
        # parts << %{ INSERT OR IGNORE INTO "#{table_name}" (#{quote_idents(target.columns)}) VALUES (#{quote_values(target.inserts)}) }
        # if target.updates.length > 0
        #   parts << %{ UPDATE "#{table_name}" SET #{quote_pairs(target.updates)} WHERE #{quote_pairs(target.selector)} }
        # end
        # parts.join(';')

      def execute(sql)
        connection.execute_batch sql
      end

      def fits_in_single_query?(take)
        take <= MAX_CONCURRENCY
      end

      def maximal?(take)
        take >= MAX_CONCURRENCY
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
