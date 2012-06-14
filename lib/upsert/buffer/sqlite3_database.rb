class Upsert
  class Buffer
    class SQLite3_Database < Buffer
      def compose(targets)
        target = targets.first
        parts = []
        parts << %{ INSERT OR IGNORE INTO "#{table_name}" (#{quote_idents(target.columns)}) VALUES (#{quote_values(target.inserts)}) }
        if target.updates.length > 0
          parts << %{ UPDATE "#{table_name}" SET #{quote_pairs(target.updates)} WHERE #{quote_pairs(target.selector)} }
        end
        parts.join(';')
      end

      def execute(sql)
        connection.execute_batch sql
      end

      def max_targets
        1
      end

      def max_length
        INFINITY
      end

      include Quoter
      
      def quote_value(v)
        case v
        when NilClass
          'NULL'
        when Symbol
          quote_value v.to_s
        when String
          SINGLE_QUOTE + SQLite3::Database.quote(v) + SINGLE_QUOTE
        when Time, DateTime
          SINGLE_QUOTE + v.strftime(ISO8601_DATETIME) + SINGLE_QUOTE
        when Date
          SINGLE_QUOTE + v.strftime(ISO8601_DATE) + SINGLE_QUOTE
        else
          v
        end
      end
      
      def quote_ident(k)
        DOUBLE_QUOTE + SQLite3::Database.quote(k.to_s) + DOUBLE_QUOTE
      end
    end
  end
end
