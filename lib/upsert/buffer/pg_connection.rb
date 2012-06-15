require 'upsert/buffer/pg_connection/column_definition'

class Upsert
  class Buffer
    class PG_Connection < Buffer
      QUOTE_VALUE = SINGLE_QUOTE
      QUOTE_IDENT = SINGLE_QUOTE
      USEC_PRECISION = true

      include Quoter

      attr_reader :merge_function

      def chunk
        return false if rows.empty?
        row = rows.shift
        unless merge_function
          create_merge_function row
        end
        hsh = row.to_hash
        ordered_args = column_definitions.map do |c|
          hsh[c.name]
        end
        %{SELECT #{merge_function}(#{quote_values(ordered_args)})}
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
      
      def create_merge_function(example_row)
        @merge_function = "pg_temp.merge_#{table_name}_#{Kernel.rand(1e11)}"
        execute <<-EOS
CREATE FUNCTION #{merge_function}(#{column_definitions.map { |c| "#{c.name}_input #{c.sql_type} DEFAULT #{c.default || 'NULL'}" }.join(',') }) RETURNS VOID AS
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
      end
    end
  end
end
