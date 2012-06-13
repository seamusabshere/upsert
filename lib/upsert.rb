require "upsert/version"

class Upsert
  INFINITY = 1.0/0
  SINGLE_QUOTE = %{'}
  BACKTICK = %{`}
  module Quoter
    def quote_idents(idents)
      idents.map { |k| quote_ident(k) }.join(',')
    end
    def quote_values(values)
      values.map { |v| quote_value(v) }.join(',')
    end
    def quote_pairs(pairs)
      pairs.map { |k, v| [quote_ident(k),quote_value(v)].join('=') }.join(',')
    end
  end

  class Buffer
    class Row
      attr_reader :selector
      attr_reader :document
      def initialize(selector, document)
        @selector = selector
        @document = document
      end
      def columns
        @columns ||= (selector.keys+document.keys).uniq
      end
      def pairs
        @pairs ||= columns.map do |k|
          value = if selector.has_key?(k)
            selector[k]
          else
            document[k]
          end
          [ k, value ]
        end
      end
      def inserts
        @inserts ||= pairs.map { |_, v| v }
      end
      def updates
        @updates ||= pairs.reject { |k, _| selector.has_key?(k) }
      end
      def to_hash
        @to_hash ||= pairs.inject({}) do |memo, (k, v)|
          memo[k.to_s] = v
          memo
        end
      end
    end

    class << self
      def for(connection, table_name)
        const_get(connection.class.name.gsub(/\W+/, '_')).new connection, table_name
      end
    end

    class PG_Connection < Buffer
      class Row < Buffer::Row; end
      
      class ColumnDefinition
        class << self
          def all(connection, table_name)
            # activerecord-3.2.5/lib/active_record/connection_adapters/postgresql_adapter.rb#column_definitions
            res = connection.exec <<-EOS
SELECT a.attname AS name, format_type(a.atttypid, a.atttypmod) AS sql_type, d.adsrc AS default
  FROM pg_attribute a LEFT JOIN pg_attrdef d
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
 WHERE a.attrelid = '#{connection.quote_ident(table_name)}'::regclass
   AND a.attnum > 0 AND NOT a.attisdropped
 ORDER BY a.attnum
 EOS
            res.map do |row|
              new row['name'], row['sql_type'], row['default']
            end
          end
        end
        attr_reader :name
        attr_reader :sql_type
        attr_reader :default
        def initialize(name, sql_type, default)
          @name = name
          @sql_type = sql_type
          @default = default
        end
      end
      attr_reader :db_function_name
      def initialize(*)
        super
        @db_function_name = "merge_#{table_name}_#{Kernel.rand(1e11)}"
      end
      def compose(targets)
        target = targets.first
        unless created_db_function?
          create_db_function target
        end
        hsh = target.to_hash
        ordered_args = column_definitions.map do |c|
          if hsh.has_key? c.name
            hsh[c.name]
          else
            nil
          end
        end
        %{ SELECT pg_temp.#{db_function_name}(#{quote_values(ordered_args)}) }
      end
      def execute(sql)
        connection.exec sql
      end
      def max_length
        INFINITY
      end
      def max_targets
        1
      end
      include Quoter
      def quote_ident(k)
        SINGLE_QUOTE + connection.quote_ident(k) + SINGLE_QUOTE
      end
      # FIXME escape_bytea with (v, k = nil)
      def quote_value(v)
        case v
        when NilClass
          'NULL'
        when String, Symbol
          SINGLE_QUOTE + connection.escape_string(v.to_s) + SINGLE_QUOTE
        else
          v
        end
      end
      def column_definitions
        @column_definitions ||= ColumnDefinition.all(connection, table_name)
      end
      private
      def created_db_function?
        !!@created_db_function_query
      end
      def create_db_function(example_row)
        execute <<-EOS
CREATE FUNCTION pg_temp.#{db_function_name}(#{column_definitions.map { |c| "#{c.name}_input #{c.sql_type} DEFAULT #{c.default || 'NULL'}" }.join(',') }) RETURNS VOID AS
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

    class Mysql2_Client < Buffer
      class Row < Buffer::Row; end
      def compose(targets)
        columns = targets.first.columns
        row_inserts = targets.map { |row| row.inserts }
        column_tautologies = columns.map do |k|
          [ quote_ident(k), "VALUES(#{quote_ident(k)})" ].join('=')
        end
        sql = <<-EOS
INSERT INTO "#{table_name}" (#{quote_idents(columns)}) VALUES (#{row_inserts.map { |row_insert| quote_values(row_insert) }.join('),(') })
ON DUPLICATE KEY UPDATE #{column_tautologies.join(',')};
EOS
        sql
      end
      def execute(sql)
        connection.query sql
      end
      def max_targets
        INFINITY
      end
      def max_length
        @max_length ||= connection.query("SHOW VARIABLES LIKE 'max_allowed_packet'", :as => :hash).first['Value'].to_i
      end
      include Quoter
      def quote_value(v)
        case v
        when NilClass
          'NULL'
        when String, Symbol
          SINGLE_QUOTE + connection.escape(v.to_s) + SINGLE_QUOTE
        else
          v
        end
      end
      def quote_ident(k)
        BACKTICK + connection.escape(k.to_s) + BACKTICK
      end
    end

    class SQLite3_Database < Buffer
      class Row < Buffer::Row
        def compose(table_name)
          sql = <<-EOS
INSERT OR IGNORE INTO "#{table_name}" (#{quote_idents(columns)}) VALUES (#{quote_values(inserts)});
UPDATE "#{table_name}" SET #{quote_pairs(updates)} WHERE #{quote_pairs(selector)}
EOS
          sql
        end
        include Quoter
        def quote_value(v)
          case v
          when NilClass
            'NULL'
          when String, Symbol
            SINGLE_QUOTE + SQLite3::Database.quote(v.to_s) + SINGLE_QUOTE
          else
            v
          end
        end
        def quote_ident(k)
          SINGLE_QUOTE + SQLite3::Database.quote(k.to_s) + SINGLE_QUOTE
        end
      end
      def compose(targets)
        targets.first.compose table_name
      end
      def execute(sql)
        connection.execute sql
      end
      def max_targets
        1
      end
      def max_length
        INFINITY
      end
    end

    attr_reader :connection
    attr_reader :table_name
    attr_reader :rows
    attr_writer :async
    def initialize(connection, table_name)
      @connection = connection
      @table_name = table_name
      @rows = []
    end
    def async?
      !!@async
    end
    def add(selector, document)
      rows << self.class.const_get(:Row).new(selector, document)
      if sql = chunk
        execute sql
      end
    end
    def clear
      while sql = chunk
        execute sql
      end
    end
    def chunk
      return if rows.empty?
      targets = []
      sql = nil
      begin
        targets << rows.pop
        last_sql = sql
        sql = compose(targets)
      end until rows.empty? or targets.length >= max_targets or sql.length > max_length
      if sql.length > max_length
        raise if last_sql.nil?
        sql = last_sql
        rows << targets.pop
      end
      sql
    end
    def cleanup
      clear
    end
  end

  attr_reader :buffer

  def initialize(connection, table_name)
    @multi_mutex = Mutex.new
    @buffer = Buffer.for connection, table_name
  end

  def row(selector, document)
    buffer.add selector, document
  end

  def cleanup
    buffer.cleanup
  end

  def multi(&blk)
    @multi_mutex.synchronize do
      begin
        buffer.async = true
        instance_eval(&blk)
        buffer.cleanup
      ensure
        buffer.async = nil
      end
    end
  end

end

=begin

=end