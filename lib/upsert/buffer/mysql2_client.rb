class Upsert
  class Buffer
    # @private
    class Mysql2_Client < Buffer
      include Quoter

      def chunk
        return if rows.empty?
        all = rows.length
        take = all
        while take > 1 and probably_oversize?(take)
          take -= 1
        end
        if async? and take == all
          return
        end
        while take > 1 and oversize?(take)
          $stderr.puts "   Length prediction via sampling failed, shrinking" if ENV['UPSERT_DEBUG'] == 'true'
          take -= 1
        end
        chunk = sql take
        while take > 1 and chunk.bytesize > max_sql_bytesize
          $stderr.puts "   Supposedly exact bytesize guess failed, shrinking" if ENV['UPSERT_DEBUG'] == 'true'
          take -= 1
          chunk = sql take
        end
        if chunk.bytesize > max_sql_bytesize
          raise TooBig
        end
        $stderr.puts "   Chunk (#{take}/#{chunk.bytesize}) was #{(chunk.bytesize / max_sql_bytesize.to_f * 100).round}% of the max" if ENV['UPSERT_DEBUG'] == 'true'
        @rows = rows.drop(take)
        chunk
      end

      def execute(sql)
        connection.query sql
      end

      def probably_oversize?(take)
        estimate_sql_bytesize(take) > max_sql_bytesize
      end

      def oversize?(take)
        sql_bytesize(take) > max_sql_bytesize
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
      def static_sql_bytesize
        @static_sql_bytesize ||= insert_part.bytesize + update_part.bytesize + 2
      end

      # where 3 is parens and comma
      def variable_sql_bytesize(take)
        rows.first(take).inject(0) { |sum, row| sum + row.values_sql_bytesize + 3 }
      end

      def estimate_variable_sql_bytesize(take)
        p = (take / 10.0).ceil
        10.0 * rows.sample(p).inject(0) { |sum, row| sum + row.values_sql_bytesize + 3 }
      end

      def sql_bytesize(take)
        static_sql_bytesize + variable_sql_bytesize(take)
      end

      def estimate_sql_bytesize(take)
        static_sql_bytesize + estimate_variable_sql_bytesize(take)
      end

      def sql(take)
        all_value_sql = rows.first(take).map { |row| row.values_sql }
        [ insert_part, '(', all_value_sql.join('),('), ')', update_part ].join
      end

      # since setting an option like :as => :hash actually persists that option to the client, don't pass any options
      def max_sql_bytesize
        @max_sql_bytesize ||= begin
          case (row = connection.query("SHOW VARIABLES LIKE 'max_allowed_packet'").first)
          when Array
            row[1]
          when Hash
            row['Value']
          else
            raise "Don't know what to do if connection.query returns a #{row.class}"
          end.to_i
        end
      end

      def quoted_value_bytesize(v)
        case v
        when NilClass
          4
        when TrueClass
          4
        when FalseClass
          5
        when BigDecimal
          v.to_s('F').bytesize
        when Upsert::Binary
          v.bytesize * 2 + 3
        when Numeric
          v.to_s.bytesize
        when String
          v.bytesize + 2
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

      # This doubles the size of the representation.
      def quote_binary(v)
        X_AND_SINGLE_QUOTE + v.unpack("H*")[0] + SINGLE_QUOTE
      end

      # put raw binary straight into sql
      # might work if we could get the encoding issues fixed when joining together the values for the sql
      # alias_method :quote_binary, :quote_string

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
