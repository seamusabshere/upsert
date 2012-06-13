class Upsert
  class Buffer
    class SQLite3_Database < Buffer
      class Row < Upsert::Row
        def compose(table_name)
          sql = <<-EOS
INSERT OR IGNORE INTO "#{table_name}" (#{quote_idents(columns)}) VALUES (#{quote_values(inserts)});
UPDATE "#{table_name}" SET #{quote_pairs(updates)} WHERE #{quote_pairs(selector)}
EOS
          sql
        end
        include Quoter
        def quote_value(v)
          case v
          when NilClass
            'NULL'
          when String, Symbol
            SINGLE_QUOTE + SQLite3::Database.quote(v.to_s) + SINGLE_QUOTE
          else
            v
          end
        end
        def quote_ident(k)
          SINGLE_QUOTE + SQLite3::Database.quote(k.to_s) + SINGLE_QUOTE
        end
      end
      def compose(targets)
        targets.first.compose table_name
      end
      def execute(sql)
        connection.execute sql
      end
      def max_targets
        1
      end
      def max_length
        INFINITY
      end
    end
  end
end
