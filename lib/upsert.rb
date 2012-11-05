require 'bigdecimal'
require 'thread'
require 'logger'

require 'upsert/version'
require 'upsert/binary'
require 'upsert/buffer'
require 'upsert/connection'
require 'upsert/row'
require 'upsert/cell'

class Upsert
  class << self
    # What logger to use.
    # @return [#info,#warn,#debug]
    attr_writer :logger
    
    # The current logger
    # @return [#info,#warn,#debug]
    def logger
      @logger || Thread.exclusive do
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

    # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
    #
    # Clear any database functions that may have been created.
    #
    # Currently only applies to PostgreSQL.
    def clear_database_functions(connection)
      dummy = new(connection, :dummy)
      dummy.buffer.clear_database_functions
    end

    # @param [String] v A string containing binary data that should be inserted/escaped as such.
    #
    # @return [Upsert::Binary]
    def binary(v)
      Binary.new v
    end

    # Guarantee that the most efficient way of buffering rows is used.
    #
    # Currently mostly helps for MySQL, but you should use it whenever possible in case future buffering-based optimizations become possible.
    #
    # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
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
    def batch(connection, table_name)
      upsert = new connection, table_name
      upsert.buffer.async!
      yield upsert
      upsert.buffer.sync!
    end

    # @deprecated Use .batch instead.
    alias :stream :batch
  end

  SINGLE_QUOTE = %{'}
  DOUBLE_QUOTE = %{"}
  BACKTICK = %{`}
  X_AND_SINGLE_QUOTE = %{x'}
  USEC_SPRINTF = '%06d'
  ISO8601_DATETIME = '%Y-%m-%d %H:%M:%S'
  ISO8601_DATE = '%F'
  NULL_WORD = 'NULL'
  HANDLER = {
    'SQLite3::Database' => 'SQLite3_Database',
    'PGConn'            => 'PG_Connection',
    'PG::Connection'    => 'PG_Connection',
    'Mysql2::Client'    => 'Mysql2_Client',
  }

  # @return [Upsert::Connection]
  attr_reader :connection

  # @return [String]
  attr_reader :table_name

  # @private
  attr_reader :buffer

  # @private
  attr_reader :row_class

  # @private
  attr_reader :cell_class

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  def initialize(connection, table_name)
    @table_name = table_name.to_s
    raw_connection = connection.respond_to?(:raw_connection) ? connection.raw_connection : connection
    connection_class_name = HANDLER[raw_connection.class.name]
    @connection = Connection.const_get(connection_class_name).new self, raw_connection
    @buffer = Buffer.const_get(connection_class_name).new self
    @row_class = Row.const_get connection_class_name
    @cell_class = Cell.const_get connection_class_name
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
  def row(selector, setter = {})
    buffer << row_class.new(self, selector, setter)
    nil
  end

  # @private
  def quoted_table_name
    @quoted_table_name ||= connection.quote_ident table_name
  end
end
