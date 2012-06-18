class Upsert
  class Buffer
    # @private
    class Mysql2_Client < Buffer
      include Quoter

      def chunk
        return if rows.empty?
        take = rows.length
        while take > 1 and not fits_in_single_query?(take)
          take -= 1
        end
        if not async? or take < rows.length
          sql = sql take
          @rows = rows.drop(take)
          sql
        end
      end

      def execute(sql)
        connection.query sql
      end

      def fits_in_single_query?(take)
        sql_length(take) <= max_sql_length
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
        rows.first(take).inject(0) { |sum, row| sum + row.values_sql_length + 3 }
      end

      def sql_length(take)
        static_sql_length + variable_sql_length(take)
      end

      def sql(take)
        all_value_sql = rows.first(take).map { |row| row.values_sql }
        [ insert_part, '(', all_value_sql.join('),('), ')', update_part ].join
      end

      def max_sql_length
        @max_sql_length ||= connection.query("SHOW VARIABLES LIKE 'max_allowed_packet'", :as => :hash).first['Value'].to_i
      end

      def quoted_value_length(v)
        case v
        when NilClass
          4
        when TrueClass
          4
        when FalseClass
          5
        when BigDecimal
          v.to_s('F').length
        when Upsert::Binary
          # conservative
          v.length * 2 + 3
        when Numeric
          v.to_s.length
        when String
          # conservative
          v.length * 2 + 2
        when Time, DateTime
          24 + 2
        when Date
          10 + 2
        else
          raise "not sure how to get quoted length of #{v.class}: #{v.inspect}"
        end
      end

      def quote_boolean(v)
        v ? 'TRUE' : 'FALSE'
      end

      def quote_string(v)
        SINGLE_QUOTE + connection.escape(v) + SINGLE_QUOTE
      end

      # We **could** do this, but I don't think it's necessary.
      # def quote_binary(v)
      #   X_AND_SINGLE_QUOTE + v.unpack("H*")[0] + SINGLE_QUOTE
      # end

      # put raw binary straight into sql
      alias_method :quote_binary, :quote_string

      def quote_time(v)
        quote_string v.strftime(ISO8601_DATETIME)
      end

      def quote_ident(k)
        BACKTICK + connection.escape(k.to_s) + BACKTICK
      end

      def quote_big_decimal(v)
        v.to_s('F')
      end
    end
  end
end
