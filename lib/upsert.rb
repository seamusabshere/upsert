require 'bigdecimal'

require 'upsert/version'
require 'upsert/binary'
require 'upsert/buffer'
require 'upsert/quoter'
require 'upsert/row'
require 'upsert/buffer/mysql2_client'
require 'upsert/buffer/pg_connection'
require 'upsert/buffer/sqlite3_database'

class Upsert
  class << self
    # @param [String] v A string containing binary data that should be inserted/escaped as such.
    #
    # @return [Upsert::Binary]
    def binary(v)
      Binary.new v
    end
  end

  # @private
  attr_reader :buffer

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  def initialize(connection, table_name)
    @multi_mutex = Mutex.new
    @buffer = Buffer.for connection, table_name
  end

  # @param [Hash] selector Key-value pairs that will be used to find or create a row.
  # @param [Hash] document Key-value pairs that will be set on the row, whether it previously existed or not.
  #
  # @return [nil]
  #
  # @example One at a time
  #   upsert = Upsert.new Pet.connection, Pet.table_name
  #   upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  #   upsert.row({:name => 'Pierre'}, :breed => 'tabby')
  def row(selector, document)
    buffer.add selector, document
    nil
  end

  # @yield [Upsert] An +Upsert+ object in "async" mode. You can call #row on it multiple times and it will try to optimize on speed.
  #
  # @return [nil]
  #
  # @example Many at once
  #   Upsert.new(Pet.connection, Pet.table_name).multi do |upsert|
  #     upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  #     upsert.row({:name => 'Pierre'}, :breed => 'tabby')
  #   end
  def multi
    @multi_mutex.synchronize do
      buffer.async = true
      yield self
      buffer.async = false
      buffer.clear
    end
    nil
  end
end
