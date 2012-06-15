class Upsert
  class Buffer
    class Mysql2_Client < Buffer
      QUOTE_VALUE = SINGLE_QUOTE
      QUOTE_IDENT = BACKTICK

      include Quoter

      def fits_in_single_query?(take)
        sql_length(take) <= max_sql_length
      end

      def maximal?(take)
        sql_length(take) >= max_sql_length
      end

      def columns
        @columns ||= rows.first.columns
      end

      def insert_part
        @insert_part ||= %{INSERT INTO "#{table_name}" (#{quote_idents(columns)}) VALUES }
      end

      def update_part
        @update_part ||= begin
          updaters = columns.map do |k|
            qk = quote_ident k
            [ qk, "VALUES(#{qk})" ].join('=')
          end.join(',')
          %{ ON DUPLICATE KEY UPDATE #{updaters}}
        end
      end

      # where 2 is the parens
      def static_sql_length
        @static_sql_length ||= insert_part.length + update_part.length + 2
      end

      # where 3 is parens and comma
      def variable_sql_length(take)
        rows.first(take).inject(0) { |sum, row| sum + row.quoted_values_length + 3 }
      end

      def sql_length(take)
        static_sql_length + variable_sql_length(take)
      end

      def sql(take)
        values = rows.first(take).map { |row| row.quoted_values }
        [ insert_part, '(', values.join('),('), ')', update_part ].join
      end

      def execute(sql)
        connection.query sql
      end

      def max_sql_length
        @max_sql_length ||= connection.query("SHOW VARIABLES LIKE 'max_allowed_packet'", :as => :hash).first['Value'].to_i
      end

      def escape_string(v)
        connection.escape v
      end
      
      def escape_ident(k)
        k
      end
    end
  end
end
