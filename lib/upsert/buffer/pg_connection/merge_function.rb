require 'digest/md5'

class Upsert
  # @private
  class Buffer
    class PG_Connection < Buffer
      class MergeFunction
        class << self
          def execute(buffer, row)
            first_try = true
            begin
              buffer.parent.connection.execute sql(buffer, row)
            rescue PG::Error => pg_error
              if first_try and pg_error.message =~ /function upsert_(.+) does not exist/
                Upsert.logger.info %{[upsert] Function #{"upsert_#{$1}".inspect} went missing, trying to recreate}
                first_try = false
                @lookup.clear
                retry
              else
                raise pg_error
              end
            end
          end

          def sql(buffer, row)
            merge_function = lookup buffer, row
            %{SELECT #{merge_function.name}(#{merge_function.values_sql(row)})}
          end

          def unique_key(table_name, selector, columns)
            [
              table_name,
              selector.join(','),
              columns.join(',')
            ].join '/'
          end

          def lookup(buffer, row)
            @lookup ||= {}
            s = row.selector.keys
            c = row.columns
            @lookup[unique_key(buffer.parent.table_name, s, c)] ||= new(buffer, s, c)
          end
        end

        attr_reader :buffer
        attr_reader :selector
        attr_reader :columns

        def initialize(buffer, selector, columns)
          @buffer = buffer
          @selector = selector
          @columns = columns
          create!
        end

        def name
          @name ||= "upsert_#{Digest::MD5.hexdigest(unique_key)}"
        end

        def values_sql(row)
          ordered_args = columns.map do |k|
            row.quoted_value(k) || NULL_WORD
          end.join(',')
        end

        private

        def unique_key
          @unique_key ||= MergeFunction.unique_key buffer.parent.table_name, selector, columns
        end

        def connection
          buffer.parent.connection
        end

        def quoted_table_name
          buffer.parent.quoted_table_name
        end

        ColumnDefinition = Struct.new(:quoted_name, :quoted_input_name, :sql_type, :default)

        # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
        def get_column_definitions
          res = connection.execute <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
FROM pg_attribute a LEFT JOIN pg_attrdef d
  ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE a.attrelid = '#{quoted_table_name}'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
EOS
          unsorted = res.select do |row|
            columns.include? row['name']
          end.inject({}) do |memo, row|
            k = row['name']
            memo[k] = ColumnDefinition.new connection.quote_ident(k), connection.quote_ident("#{k}_input"), row['sql_type'], row['default']
            memo
          end
          columns.map do |k|
            unsorted[k]
          end
        end

        # the "canonical example" from http://www.postgresql.org/docs/9.1/static/plpgsql-control-structures.html#PLPGSQL-UPSERT-EXAMPLE
        def create!
          Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{buffer.parent.table_name.inspect} for selector #{selector.map(&:inspect).join(', ')} and columns #{columns.map(&:inspect).join(', ')}"
          column_definitions = get_column_definitions
          connection.execute <<-EOS
CREATE OR REPLACE FUNCTION #{name}(#{column_definitions.map { |c| "#{c.quoted_input_name} #{c.sql_type} DEFAULT #{c.default || 'NULL'}" }.join(',') }) RETURNS VOID AS
$$
BEGIN
  LOOP
      -- first try to update the key
      UPDATE #{quoted_table_name} SET #{column_definitions.map { |c| "#{c.quoted_name} = #{c.quoted_input_name}" }.join(',')}
          WHERE #{selector.map { |k| "#{connection.quote_ident(k)} = #{connection.quote_ident([k,'input'].join('_'))}" }.join(' AND ') };
      IF found THEN
          RETURN;
      END IF;
      -- not there, so try to insert the key
      -- if someone else inserts the same key concurrently,
      -- we could get a unique-key failure
      BEGIN
          INSERT INTO #{quoted_table_name}(#{column_definitions.map { |c| c.quoted_name }.join(',')}) VALUES (#{column_definitions.map { |c| c.quoted_input_name }.join(',')});
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
end
