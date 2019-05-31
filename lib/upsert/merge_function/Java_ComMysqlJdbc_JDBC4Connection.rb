require "upsert/merge_function/mysql"

class Upsert
  class MergeFunction
    # @private
    class Java_ComMysqlJdbc_JDBC4Connection < MergeFunction
      include Mysql

      def sql
        @sql ||= begin
          bind_params = Array.new(selector_keys.length + setter_keys.length, "?")
          %{CALL #{name}(#{bind_params.join(", ")})}
        end
      end

      def execute(row)
        first_try = true
        bind_selector_values = row.selector.values.map { |v| connection.bind_value v }
        bind_setter_values = row.setter.values.map { |v| connection.bind_value v }
        begin
          connection.execute sql, (bind_selector_values + bind_setter_values)
        rescue com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException => e
          if e.message =~ /PROCEDURE.*does not exist/i
            if first_try
              Upsert.logger.info %([upsert] Function #{name.inspect} went missing, trying to recreate)
              first_try = false
              create!
              retry
            else
              Upsert.logger.info %([upsert] Failed to create function #{name.inspect} for some reason)
              raise e
            end
          else
            raise e
          end
        end
      end
    end
  end
end
