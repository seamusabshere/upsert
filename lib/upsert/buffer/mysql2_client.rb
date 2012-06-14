class Upsert
  class Buffer
    class Mysql2_Client < Buffer
      def compose(targets)
        columns = targets.first.columns
        row_inserts = targets.map { |row| row.inserts }
        column_tautologies = columns.map do |k|
          [ quote_ident(k), "VALUES(#{quote_ident(k)})" ].join('=')
        end
        sql = <<-EOS
INSERT INTO "#{table_name}" (#{quote_idents(columns)}) VALUES (#{row_inserts.map { |row_insert| quote_values(row_insert) }.join('),(') })
ON DUPLICATE KEY UPDATE #{column_tautologies.join(',')};
EOS
        sql
      end

      def execute(sql)
        connection.query sql
      end

      def max_targets
        INFINITY
      end

      def max_length
        @max_length ||= connection.query("SHOW VARIABLES LIKE 'max_allowed_packet'", :as => :hash).first['Value'].to_i
      end

      include Quoter

      def quote_value(v)
        case v
        when NilClass
          'NULL'
        when Symbol
          quote_value v.to_s
        when String
          SINGLE_QUOTE + connection.escape(v) + SINGLE_QUOTE
        when Time, DateTime
          SINGLE_QUOTE + v.strftime(ISO8601_DATETIME) + SINGLE_QUOTE
        when Date
          SINGLE_QUOTE + v.strftime(ISO8601_DATE) + SINGLE_QUOTE
        else
          v
        end
      end
      
      def quote_ident(k)
        BACKTICK + connection.escape(k.to_s) + BACKTICK
      end
    end
  end
end
