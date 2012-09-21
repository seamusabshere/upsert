class Upsert
  # @private
  class Buffer
    class Mysql2_Client < Buffer
      def ready
        return if rows.empty?
        connection = parent.connection
        if not async?
          connection.execute sql
          rows.clear
          return
        end
        @cumulative_sql_bytesize ||= static_sql_bytesize
        new_row = rows.pop
        d = new_row.values_sql_bytesize + 3 # ),(
        if @cumulative_sql_bytesize + d > max_sql_bytesize
          connection.execute sql
          rows.clear
          @cumulative_sql_bytesize = static_sql_bytesize + d
        else
          @cumulative_sql_bytesize += d
        end
        rows << new_row
        nil
      end

      def setter
        @setter ||= rows.first.setter.keys
      end

      def original_setter
        @original_setter ||= rows.first.original_setter_keys
      end

      def insert_part
        @insert_part ||= begin
          connection = parent.connection
          column_names = setter.map { |k| connection.quote_ident(k) }
          %{INSERT INTO #{parent.quoted_table_name} (#{column_names.join(',')}) VALUES }
        end
      end

      def update_part
        @update_part ||= begin
          connection = parent.connection
          updaters = setter.map do |k|
            quoted_name = connection.quote_ident(k)
            if original_setter.include?(k)
              "#{quoted_name}=VALUES(#{quoted_name})"
            else
              # NOOP
              "#{quoted_name}=#{quoted_name}"
            end
          end.join(',')
          %{ ON DUPLICATE KEY UPDATE #{updaters}}
        end
      end

      # where 2 is the parens
      def static_sql_bytesize
        @static_sql_bytesize ||= insert_part.bytesize + update_part.bytesize + 2
      end

      def sql
        [
          insert_part,
          '(',
          rows.map { |row| row.quoted_setter_values.join(',') }.join('),('),
          ')',
          update_part
        ].join
      end

      # since setting an option like :as => :hash actually persists that option to the client, don't pass any options
      def max_sql_bytesize
        @max_sql_bytesize ||= parent.connection.database_variable_get(:MAX_ALLOWED_PACKET).to_i
      end
    end
  end
end
