require 'upsert/buffer/pg_connection/column_definition'

class Upsert
  class Buffer
    class PG_Connection < Buffer
      MAX_CONCURRENCY = 1
      QUOTE_VALUE = SINGLE_QUOTE
      QUOTE_IDENT = SINGLE_QUOTE

      include Quoter

      attr_reader :db_function_name

        # unless created_db_function?
        #   create_db_function target
        # end
        # hsh = target.to_hash
        # ordered_args = column_definitions.map do |c|
        #   if hsh.has_key? c.name
        #     hsh[c.name]
        #   else
        #     nil
        #   end
        # end
        # %{ SELECT #{db_function_name}(#{quote_values(ordered_args)}) }

      def fits_in_single_query?(take)
        take <= MAX_CONCURRENCY
      end

      def maximal?(take)
        take >= MAX_CONCURRENCY
      end

      def execute(sql)
        connection.exec sql
      end

      def escape_ident(k)
        connection.quote_ident k
      end

      # FIXME escape_bytea with (v, k = nil)
      def escape_string(v)
        connection.escape_string v
      end
      
      def column_definitions
        @column_definitions ||= ColumnDefinition.all(connection, table_name)
      end
      
      private
      
      def created_db_function?
        !!@created_db_function_query
      end
      
      def create_db_function(example_row)
        @db_function_name = "pg_temp.merge_#{table_name}_#{Kernel.rand(1e11)}"
        execute <<-EOS
CREATE FUNCTION #{db_function_name}(#{column_definitions.map { |c| "#{c.name}_input #{c.sql_type} DEFAULT #{c.default || 'NULL'}" }.join(',') }) RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE #{table_name} SET #{column_definitions.map { |c| "#{c.name} = #{c.name}_input" }.join(',')} WHERE #{example_row.selector.keys.map { |k| "#{k} = #{k}_input" }.join(' AND ') };
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO #{table_name}(#{column_definitions.map { |c| c.name }.join(',')}) VALUES (#{column_definitions.map { |c| "#{c.name}_input" }.join(',')});
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
EOS
        @created_db_function_query = true
      end
    end
  end
end
