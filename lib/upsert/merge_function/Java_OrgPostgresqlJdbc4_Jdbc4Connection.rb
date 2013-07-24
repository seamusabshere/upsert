require 'upsert/merge_function/postgresql'

class Upsert
  class MergeFunction
    # @private
    class Java_OrgPostgresqlJdbc4_Jdbc4Connection < MergeFunction
      include Postgresql

      def execute(row)
        first_try = true
        values = []
        values += row.selector.values
        values += row.setter.values
        hstore_delete_handlers.each do |hstore_delete_handler|
          values << row.hstore_delete_keys.fetch(hstore_delete_handler.name, [])
        end
        begin
          connection.execute sql, values.map { |v| connection.bind_value v }
        rescue org.postgresql.util.PSQLException => pg_error
          if pg_error.message =~ /function #{name}.* does not exist/i
            if first_try
              Upsert.logger.info %{[upsert] Function #{name.inspect} went missing, trying to recreate}
              first_try = false
              create!
              retry
            else
              Upsert.logger.info %{[upsert] Failed to create function #{name.inspect} for some reason}
              raise pg_error
            end
          else
            raise pg_error
          end
        end
      end


    end
  end
end
