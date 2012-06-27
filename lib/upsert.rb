require 'bigdecimal'

require 'upsert/version'
require 'upsert/binary'
require 'upsert/row'
require 'upsert/mysql2_client'
require 'upsert/pg_connection'
require 'upsert/sqlite3_database'

class Upsert
  class << self
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
    # @raise [Upsert::TooBig] If any row is too big to fit inside a single packet.
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
      upsert.async!
      yield upsert
      upsert.sync!
    end

    # @deprecated Use .batch instead.
    alias :stream :batch
  end

  # Raised if a query would be too large to send in a single packet.
  class TooBig < RuntimeError
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

  # @return [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection]
  attr_reader :connection

  # @return [String,Symbol]
  attr_reader :table_name

  # @private
  attr_reader :buffer

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  def initialize(connection, table_name)
    @table_name = table_name
    @buffer = []

    @connection = if connection.respond_to?(:raw_connection)
      # deal with ActiveRecord::Base.connection or ActiveRecord::Base.connection_pool.checkout
       connection.raw_connection
    else
      connection
    end

    extend Upsert.const_get(@connection.class.name.gsub(/\W+/, '_'))
  end

  # Upsert a row given a selector and a document.
  #
  # @see http://api.mongodb.org/ruby/1.6.4/Mongo/Collection.html#update-instance_method Loosely based on the upsert functionality of the mongo-ruby-driver #update method
  #
  # @param [Hash] selector Key-value pairs that will be used to find or create a row.
  # @param [Hash] document Key-value pairs that will be set on the row, whether it previously existed or not.
  #
  # @raise [Upsert::TooBig] If any row is too big to fit inside a single packet.
  #
  # @return [nil]
  #
  # @example One at a time
  #   upsert = Upsert.new Pet.connection, Pet.table_name
  #   upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  #   upsert.row({:name => 'Pierre'}, :breed => 'tabby')
  def row(selector, document = {})
    buffer.push Row.new(self, selector, document)
    if sql = chunk
      execute sql
    end
    nil
  end

  # @private
  def async?
    !!@async
  end

  # @private
  def async!
    @async = true
  end

  # @private
  def sync!
    @async = false
    while sql = chunk
      execute sql
    end
  end

  # @private
  def quote_value(v)
    case v
    when NilClass
      NULL_WORD
    when Upsert::Binary
      quote_binary v # must be defined by base
    when String
      quote_string v # must be defined by base
    when TrueClass, FalseClass
      quote_boolean v
    when BigDecimal
      quote_big_decimal v
    when Numeric
      v
    when Symbol
      quote_string v.to_s
    when Time, DateTime
      quote_time v # must be defined by base
    when Date
      quote_string v.strftime(ISO8601_DATE)
    else
      raise "not sure how to quote #{v.class}: #{v.inspect}"
    end
  end
end
