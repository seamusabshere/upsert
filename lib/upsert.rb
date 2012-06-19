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

    # @yield [Upsert] An +Upsert+ object in streaming mode. You can call #row on it multiple times and it will try to optimize on speed.
    #
    # @note Buffered in memory until it's efficient to send to the server a packet.
    #
    # @raise [Upsert::TooBig] If any row is too big to fit inside a single packet.
    #
    # @return [nil]
    #
    # @example Many at once
    #   Upsert.stream(Pet.connection, Pet.table_name) do |upsert|
    #     upsert.row({:name => 'Jerry'}, :breed => 'beagle')
    #     upsert.row({:name => 'Pierre'}, :breed => 'tabby')
    #   end
    def stream(connection, table_name)
      upsert = new connection, table_name
      upsert.buffer.async!
      yield upsert
      upsert.buffer.sync!
    end
  end

  # Raised if a query would be too large to send in a single packet.
  class TooBig < RuntimeError
  end

  # @private
  attr_reader :buffer

  # @param [Mysql2::Client,Sqlite3::Database,PG::Connection,#raw_connection] connection A supported database connection.
  # @param [String,Symbol] table_name The name of the table into which you will be upserting.
  def initialize(connection, table_name)
    @buffer = Buffer.for connection, table_name
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
  def row(selector, document)
    buffer.add selector, document
    nil
  end
end
