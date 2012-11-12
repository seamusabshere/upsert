class Upsert
  class MergeFunction
    # @private
    class SQLite3_Database < MergeFunction
      attr_reader :quoted_setter_names
      attr_reader :quoted_selector_names

      def initialize(*)
        super
        @quoted_setter_names = setter_keys.map { |k| connection.quote_ident k }
        @quoted_selector_names = selector_keys.map { |k| connection.quote_ident k }
      end

      def create!
        # not necessary
      end

      def execute(row)
        bind_setter_values = row.setter.values.map(&:bind_value)
        
        insert_or_ignore_sql = %{INSERT OR IGNORE INTO #{quoted_table_name} (#{quoted_setter_names.join(',')}) VALUES (#{Array.new(bind_setter_values.length, '?').join(',')})}
        connection.execute insert_or_ignore_sql, bind_setter_values

        update_sql = %{UPDATE #{quoted_table_name} SET #{quoted_setter_names.map { |qk| "#{qk}=?" }.join(',')} WHERE #{quoted_selector_names.map { |qk| "#{qk}=?" }.join(' AND ')}}
        connection.execute update_sql, (bind_setter_values + row.selector.values.map(&:bind_value))
      end
    end
  end
end
