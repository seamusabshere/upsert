class Upsert
  # @private
  module Mysql2_Client
    def chunk
      return if buffer.empty?
      if not async?
        retval = sql
        buffer.clear
        return retval
      end
      @cumulative_sql_bytesize ||= static_sql_bytesize
      new_row = buffer.pop
      d = new_row.values_sql_bytesize + 3 # ),(
      if @cumulative_sql_bytesize + d > max_sql_bytesize
        retval = sql
        buffer.clear
        @cumulative_sql_bytesize = static_sql_bytesize + d
      else
        retval = nil
        @cumulative_sql_bytesize += d
      end
      buffer.push new_row
      retval
    end

    def execute(sql)
      connection.query sql
    end

    def columns
      @columns ||= buffer.first.columns
    end

    def insert_part
      @insert_part ||= %{INSERT INTO #{quote_ident(table_name)} (#{columns.join(',')}) VALUES }
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

    def sql
      all_value_sql = buffer.map { |row| row.values_sql }
      retval = [ insert_part, '(', all_value_sql.join('),('), ')', update_part ].join
      retval
    end

    # since setting an option like :as => :hash actually persists that option to the client, don't pass any options
    def max_sql_bytesize
      @max_sql_bytesize ||= database_variable_get(:MAX_ALLOWED_PACKET).to_i
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

    def database_variable_get(k)
      case (row = connection.query("SHOW VARIABLES LIKE '#{k}'").first)
      when Array
        row[1]
      when Hash
        row['Value']
      else
        raise "Don't know what to do if connection.query returns a #{row.class}"
      end
    end
  end
end
