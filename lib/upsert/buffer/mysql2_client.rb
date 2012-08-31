class Upsert
  # @private
  class Buffer
    class Mysql2_Client < Buffer
      def ready
        return if rows.empty?
        c = parent.connection
        if not async?
          c.execute sql
          rows.clear
          return
        end
        @cumulative_sql_bytesize ||= static_sql_bytesize
        new_row = rows.pop
        d = new_row.values_sql_bytesize + 3 # ),(
        if @cumulative_sql_bytesize + d > max_sql_bytesize
          c.execute sql
          rows.clear
          @cumulative_sql_bytesize = static_sql_bytesize + d
        else
          @cumulative_sql_bytesize += d
        end
        rows << new_row
        nil
      end

      def columns
        @columns ||= rows.first.columns
      end

      def insert_part
        @insert_part ||= begin
          connection = parent.connection
          columns_sql = columns.map { |k| connection.quote_ident(k) }.join(',')
          %{INSERT INTO #{parent.quoted_table_name} (#{columns_sql}) VALUES }
        end
      end

      def update_part
        @update_part ||= begin
          connection = parent.connection
          updaters = columns.map do |k|
            qk = connection.quote_ident(k)
            [ qk, "VALUES(#{qk})" ].join('=')
          end.join(',')
          %{ ON DUPLICATE KEY UPDATE #{updaters}}
        end
      end

      # where 2 is the parens
      def static_sql_bytesize
        @static_sql_bytesize ||= insert_part.bytesize + update_part.bytesize + 2
      end

      def sql
        all_value_sql = rows.map { |row| row.values_sql }
        retval = [ insert_part, '(', all_value_sql.join('),('), ')', update_part ].join
        retval
      end

      # since setting an option like :as => :hash actually persists that option to the client, don't pass any options
      def max_sql_bytesize
        @max_sql_bytesize ||= parent.connection.database_variable_get(:MAX_ALLOWED_PACKET).to_i
      end
    end
  end
end
