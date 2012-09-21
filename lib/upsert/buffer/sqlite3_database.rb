class Upsert
  class Buffer
    # @private
    class SQLite3_Database < Buffer
      def ready
        return if rows.empty?
        row = rows.shift
        connection = parent.connection
        bind_setter_values = row.setter.values.map(&:bind_value)
        quoted_setter_names = row.setter.values.map(&:quoted_name)
        quoted_selector_names = row.selector.values.map(&:quoted_name)

        insert_or_ignore_sql = %{INSERT OR IGNORE INTO #{parent.quoted_table_name} (#{quoted_setter_names.join(',')}) VALUES (#{Array.new(bind_setter_values.length, '?').join(',')})}
        connection.execute insert_or_ignore_sql, bind_setter_values

        update_sql = %{UPDATE #{parent.quoted_table_name} SET #{quoted_setter_names.map { |qk| "#{qk}=?" }.join(',')} WHERE #{quoted_selector_names.map { |qk| "#{qk}=?" }.join(' AND ')}}
        connection.execute update_sql, (bind_setter_values + row.selector.values.map(&:bind_value))
      end
    end
  end
end
