class Upsert
  class Buffer
    # @private
    class SQLite3_Database < Buffer
      def ready
        return if rows.empty?
        row = rows.shift
        c = parent.connection
        c.execute %{INSERT OR IGNORE INTO #{parent.quoted_table_name} (#{row.columns_sql}) VALUES (#{row.values_sql}); UPDATE #{parent.quoted_table_name} SET #{row.set_sql} WHERE #{row.where_sql}}
      end
    end
  end
end
