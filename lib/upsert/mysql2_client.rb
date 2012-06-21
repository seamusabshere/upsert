class Upsert
  # @private
  module Mysql2_Client
    SAMPLE = 0.1

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
      while take > 2 and oversize?(take)
        $stderr.puts "   Length prediction via sampling failed, shrinking" if ENV['UPSERT_DEBUG'] == 'true'
        take -= 2
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
      @insert_part ||= %{INSERT INTO "#{table_name}" (#{columns.join(',')}) VALUES }
    end

    def update_part
      @update_part ||= begin
        updaters = columns.map do |k|
          [ k, "VALUES(#{k})" ].join('=')
        end.join(',')
        %{ ON DUPLICATE KEY UPDATE #{updaters}}
      end
    end

    # where 2 is the parens
    def static_sql_bytesize
      @static_sql_bytesize ||= insert_part.bytesize + update_part.bytesize + 2
    end

    
    def variable_sql_bytesize(take)
      memo = rows.first(take).inject(0) { |sum, row| sum + row.values_sql_bytesize }
      if take > 0
        # parens and comma
        memo += 3*(take-1)
      end
      memo
    end

    def estimate_variable_sql_bytesize(take)
      n = (take * SAMPLE).ceil
      sample = if RUBY_VERSION >= '1.9'
        rows.first(take).sample(n)
      else
        # based on https://github.com/marcandre/backports/blob/master/lib/backports/1.8.7/array.rb
        memo = rows.first(take)
        n.times do |i|
          r = i + Kernel.rand(take - i)
          memo[i], memo[r] = memo[r], memo[i]
        end
        memo.first(n)
      end
      memo = sample.inject(0) { |sum, row| sum + row.values_sql_bytesize } / SAMPLE
      if take > 0
        # parens and comma
        memo += 3*(take-1)
      end
      memo
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
