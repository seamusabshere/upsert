require 'bigdecimal'
require 'thread'
require 'logger'

require 'upsert/version'
require 'upsert/binary'
require 'upsert/connection'
require 'upsert/merge_function'
require 'upsert/column_definition'
require 'upsert/row'

class Upsert
  class << self
    # What logger to use.
    # @return [#info,#warn,#debug]
    attr_writer :logger
    MUTEX_FOR_PERFORM = Mutex.new

    # The current logger
    # @return [#info,#warn,#debug]
    def logger
      @logger || MUTEX_FOR_PERFORM.synchronize do
        @logger ||= if defined?(::Rails) and (rails_logger = ::Rails.logger)
          rails_logger
        elsif defined?(::ActiveRecord) and ::ActiveRecord.const_defined?(:Base) and (ar_logger = ::ActiveRecord::Base.logger)
          ar_logger
        else
          my_logger = Logger.new $stderr
          case ENV['UPSERT_DEBUG']
          when 'true'
            my_logger.level = Logger::DEBUG
          when 'false'
            my_logger.level = Logger::INFO
          end
          my_logger
        end
      end
    end

    def mutex_for_row(upsert, row)
      retrieve_mutex(upsert.table_name, row.selector.keys)
    end

    def mutex_for_function(upsert, row)
      retrieve_mutex(upsert.table_name, row.selector.keys, row.setter.keys)
    end

    # TODO: Rewrite this to use the thread_safe gem, perhaps?
    def retrieve_mutex(*args)
      # ||= isn't an atomic operation
      MUTEX_FOR_PERFORM.synchronize do
        @mutex_cache ||= {}
      end

      @mutex_cache.fetch(args.flatten.join('::')) do |k|
        MUTEX_FOR_PERFORM.synchronize do
          # We still need the ||= because this block could have
          # theoretically been entered simultaneously by two threads
          # but the actual assignment is protected by the mutex
          @mutex_cache[k] ||= Mutex.new
        end
      end
    end

    # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#metal] connection A supported database connection.
    #
    # Clear any database functions that may have been created.
    #
    # Currently only applies to PostgreSQL.
    def clear_database_functions(connection)
      dummy = new(connection, :dummy)
      dummy.clear_database_functions
    end

    # @param [String] v A string containing binary data that should be inserted/escaped as such.
    #
    # @return [Upsert::Binary]
    def binary(v)
      Binary.new v
    end

    # More efficient way of upserting multiple rows at once.
    #
    # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#metal] connection A supported database connection.
    # @param [String,Symbol] table_name The name of the table into which you will be upserting.
    #
    # @yield [Upsert] An +Upsert+ object in batch mode. You can call #row on it multiple times and it will try to optimize on speed.
    #
    # @return [nil]
    #
    # @example Many at once
    #   Upsert.batch(Pet.connection, Pet.table_name) do |upsert|
    #     upsert.row({:name => 'Jerry'}, :breed => 'beagle')
    #     upsert.row({:name => 'Pierre'}, :breed => 'tabby')
    #   end
    def batch(connection, table_name, options = {})
      upsert = new connection, table_name, options
      yield upsert
    end

    # @deprecated Use .batch instead.
    alias :stream :batch

    # @private
    def class_name(metal)
      if RUBY_PLATFORM == 'java'
        metal.class.name || metal.get_class.name
      else
        metal.class.name
      end
    end

    # @private
    def flavor(metal)
      case class_name(metal)
      when /sqlite/i
        'Sqlite3'
      when /mysql/i
        'Mysql'
      when /pg/i, /postgres/i
        'Postgresql'
      else
        raise "[upsert] #{metal} not supported"
      end
    end

    # @private
    def adapter(metal)
      metal_class_name = class_name metal
      METAL_CLASS_ALIAS.fetch(metal_class_name, metal_class_name).gsub /\W+/, '_'
    end

    # @private
    def metal(connection)
      metal = connection.respond_to?(:raw_connection) ? connection.raw_connection : connection
      if metal.class.name.to_s.start_with?('ActiveRecord::ConnectionAdapters')
        metal = metal.connection
      end
      metal
    end

    # @private
    def utc(time)
      if time.is_a? DateTime
        usec = time.sec_fraction * SEC_FRACTION
        if time.offset != 0
          time = time.new_offset(0)
        end
        Time.utc time.year, time.month, time.day, time.hour, time.min, time.sec, usec
      elsif time.utc?
        time
      else
        time.utc
      end
    end

    # @private
    def utc_iso8601(time, tz = true)
      t = utc time
      s = t.strftime(ISO8601_DATETIME) + '.' + (USEC_SPRINTF % t.usec)
      tz ? (s + UTC_TZ) : s
    end
  end

  SINGLE_QUOTE = %{'}
  DOUBLE_QUOTE = %{"}
  BACKTICK = %{`}
  X_AND_SINGLE_QUOTE = %{x'}
  USEC_SPRINTF = '%06d'
  if RUBY_VERSION >= '1.9.0'
    SEC_FRACTION = 1e6
    NANO_FRACTION = 1e9
  else
    SEC_FRACTION = 8.64e10
    NANO_FRACTION = 8.64e13
  end
  ISO8601_DATETIME = '%Y-%m-%d %H:%M:%S'
  ISO8601_DATE = '%F'
  UTC_TZ = '+00:00'
  NULL_WORD = 'NULL'
  METAL_CLASS_ALIAS = {
    'PGConn'                     => 'PG::Connection',
    'org.sqlite.Conn'            => 'Java::OrgSqlite::Conn', # for some reason, org.sqlite.Conn doesn't have a ruby class name
    'Sequel::Postgres::Adapter'  => 'PG::Connection',      # Only the Postgres adapter needs an alias
  }
  CREATED_COL_REGEX = /\Acreated_(at|on)\z/

  # @return [Upsert::Connection]
  attr_reader :connection

  # @return [String]
  attr_reader :table_name

  # @private
  attr_reader :merge_function_class

  # @private
  attr_reader :flavor

  # @private
  attr_reader :adapter

  # @private
  def assume_function_exists?
    @assume_function_exists
  end

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#metal] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  # @param [Hash] options
  # @option options [TrueClass,FalseClass] :assume_function_exists (true) Assume the function has already been defined correctly by another process.
  def initialize(connection, table_name, options = {})
    @table_name = self.class.normalize_table_name(table_name)
    metal = Upsert.metal connection
    @flavor = Upsert.flavor metal
    @adapter = Upsert.adapter metal
    # todo memoize
    Dir[File.expand_path("../upsert/**/{#{flavor.downcase},#{adapter}}.rb", __FILE__)].each do |path|
      require path
    end
    @connection = Connection.const_get(adapter).new self, metal
    @merge_function_class = MergeFunction.const_get adapter
    @merge_function_cache = {}
    @assume_function_exists = options.fetch :assume_function_exists, @flavor != "Postgresql"

    @merge_function_mutex = Mutex.new
    @row_mutex = Mutex.new
  end

  # Upsert a row given a selector and a setter.
  #
  # The selector values are used as setters if it's a new row. So if your selector is `name=Jerry` and your setter is `age=4`, and there is no Jerry yet, then a new row will be created with name Jerry and age 4.
  #
  # @see http://api.mongodb.org/ruby/1.6.4/Mongo/Collection.html#update-instance_method Loosely based on the upsert functionality of the mongo-ruby-driver #update method
  #
  # @param [Hash] selector Key-value pairs that will be used to find or create a row.
  # @param [Hash] setter Key-value pairs that will be set on the row, whether it previously existed or not.
  #
  # @return [nil]
  #
  # @example One at a time
  #   upsert = Upsert.new Pet.connection, Pet.table_name
  #   upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  #   upsert.row({:name => 'Pierre'}, :breed => 'tabby')
  def row(selector, setter = {}, options = nil)
    row_object = Row.new(selector, setter, options)
    self.class.mutex_for_row(self, row_object).synchronize do
      merge_function(row_object).execute(row_object)
      nil
    end
  end

  # @private
  def clear_database_functions
    merge_function_class.clear connection
  end

  def merge_function(row)
    cache_key = [quoted_table_name, row.selector.keys, row.setter.keys]
    self.class.mutex_for_function(self, row).synchronize do
      @merge_function_cache[cache_key] ||=
        merge_function_class.new(self, row.selector.keys, row.setter.keys, assume_function_exists?)
    end
  end

  # @private
  def quoted_table_name
    @quoted_table_name ||= table_name.map { |t| connection.quote_ident(t) }.join(".")
  end

  # @private
  def column_definitions
    @column_definitions ||= ColumnDefinition.const_get(flavor).all connection, quoted_table_name
  end

  # @private
  def self.normalize_table_name(table_name)
    if defined?(Sequel) && table_name.is_a?(::Sequel::SQL::QualifiedIdentifier)
      [table_name.table, table_name.column]
    else
      [*table_name].map(&:to_s)
    end
  end
end
