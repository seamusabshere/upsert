require 'upsert/pg_connection/column_definition'

class Upsert
  # @private
  module PG_Connection

    attr_reader :columns
    attr_reader :merge_function

    def chunk
      return if buffer.empty?
      row = buffer.shift
      unless @columns.is_a?(Array)
        @columns = row.columns
      end
      unless merge_function
        create_merge_function row
      end
      hsh = row.to_hash
      ordered_args = column_definitions.map do |c|
        hsh[c.name] || NULL_WORD
      end
      %{SELECT #{merge_function}(#{ordered_args.join(',')})}
    end

    def execute(sql)
      Upsert.logger.debug %{Upsert: #{sql}}
      connection.exec sql
    end

    def quote_string(v)
      SINGLE_QUOTE + connection.escape_string(v) + SINGLE_QUOTE
    end

    def quote_binary(v)
      E_AND_SINGLE_QUOTE + connection.escape_bytea(v) + SINGLE_QUOTE
    end

    def quote_time(v)
      quote_string [v.strftime(ISO8601_DATETIME), sprintf(USEC_SPRINTF, v.usec)].join('.')
    end

    def quote_big_decimal(v)
      v.to_s('F')
    end

    def quote_boolean(v)
      v ? 'TRUE' : 'FALSE'
    end

    def quote_ident(k)
      connection.quote_ident k.to_s
    end

    def column_definitions
      @column_definitions ||= ColumnDefinition.all(connection, table_name).select { |cd| columns.include?(cd.name) }
    end
    
    private
    
    def create_merge_function(example_row)
      @merge_function = "pg_temp.merge_#{table_name}_#{Kernel.rand(1e11)}"
      execute <<-EOS
CREATE FUNCTION #{merge_function}(#{column_definitions.map { |c| "#{c.input_name} #{c.sql_type} DEFAULT #{c.default || 'NULL'}" }.join(',') }) RETURNS VOID AS
$$
BEGIN
  LOOP
      -- first try to update the key
      UPDATE #{quote_ident(table_name)} SET #{column_definitions.map { |c| "#{c.name} = #{c.input_name}" }.join(',')} WHERE #{example_row.raw_selector.keys.map { |k| "#{quote_ident(k)} = #{quote_ident([k,'input'].join('_'))}" }.join(' AND ') };
      IF found THEN
          RETURN;
      END IF;
      -- not there, so try to insert the key
      -- if someone else inserts the same key concurrently,
      -- we could get a unique-key failure
      BEGIN
          INSERT INTO #{quote_ident(table_name)}(#{column_definitions.map { |c| c.name }.join(',')}) VALUES (#{column_definitions.map { |c| c.input_name }.join(',')});
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

  # @private
  # backwards compatibility - https://github.com/seamusabshere/upsert/issues/2
  PGconn = PG_Connection
end
