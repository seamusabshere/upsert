require 'bigdecimal'
require 'thread'
require 'logger'

require 'upsert/version'
require 'upsert/binary'
require 'upsert/buffer'
require 'upsert/connection'
require 'upsert/row'

class Upsert
  class << self
    # What logger to use.
    # @return [#info,#warn,#debug]
    attr_writer :logger
    
    # The current logger
    # @return [#info,#warn,#debug]
    def logger
      @logger || Thread.exclusive do
        @logger ||= if defined?(::Rails) and (rails_logger = Rails.logger)
          rails_logger
        else
          my_logger = Logger.new $stderr
          my_logger.level = Logger::INFO
          my_logger
        end
        if ENV['UPSERT_DEBUG'] == 'true'
          @logger.level = Logger::DEBUG
        end
        @logger
      end
    end

    # @param [String] v A string containing binary data that should be inserted/escaped as such.
    #
    # @return [Upsert::Binary]
    def binary(v)
      Binary.new v
    end

    # @yield [Upsert] An +Upsert+ object in batch mode. You can call #row on it multiple times and it will try to optimize on speed.
    #
    # @note Buffered in memory until it's efficient to send to the server a packet.
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
  E_AND_SINGLE_QUOTE = %{E'}
  X_AND_SINGLE_QUOTE = %{x'}
  USEC_SPRINTF = '%06d'
  ISO8601_DATETIME = '%Y-%m-%d %H:%M:%S'
  ISO8601_DATE = '%F'
  NULL_WORD = 'NULL'

  # @return [Upsert::Connection]
  attr_reader :connection

  # @return [String]
  attr_reader :table_name

  # @private
  attr_reader :buffer

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  def initialize(connection, table_name)
    @table_name = table_name.to_s
    raw_connection = connection.respond_to?(:raw_connection) ? connection.raw_connection : connection
    n = raw_connection.class.name.gsub(/\W+/, '_')
    @connection = Connection.const_get(n).new self, raw_connection
    @buffer = Buffer.const_get(n).new self
  end

  # Upsert a row given a selector and a document.
  #
  # @see http://api.mongodb.org/ruby/1.6.4/Mongo/Collection.html#update-instance_method Loosely based on the upsert functionality of the mongo-ruby-driver #update method
  #
  # @param [Hash] selector Key-value pairs that will be used to find or create a row.
  # @param [Hash] document Key-value pairs that will be set on the row, whether it previously existed or not.
  #
  # @return [nil]
  #
  # @example One at a time
  #   upsert = Upsert.new Pet.connection, Pet.table_name
  #   upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  #   upsert.row({:name => 'Pierre'}, :breed => 'tabby')
  def row(selector, document = {})
    buffer << Row.new(self, selector, document)
    nil
  end

  # @private
  def quoted_table_name
    @quoted_table_name ||= connection.quote_ident table_name
  end
end
