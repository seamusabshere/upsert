require "upsert/merge_function/mysql"

class Upsert
  class MergeFunction
    # @private
    class Mysql2_Client < MergeFunction
      include Mysql

      def sql(row)
        quoted_params = (row.selector.values + row.setter.values).map { |v| connection.quote_value v }
        %{CALL #{name}(#{quoted_params.join(", ")})}
      end

      def execute(row)
        first_try = true
        begin
          connection.execute sql(row)
        rescue Mysql2::Error => e
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
