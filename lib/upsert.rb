require 'upsert/version'
require 'upsert/buffer'
require 'upsert/quoter'
require 'upsert/row'
require 'upsert/buffer/mysql2_client'
require 'upsert/buffer/pg_connection'
require 'upsert/buffer/sqlite3_database'

class Upsert
  attr_reader :buffer

  def initialize(connection, table_name)
    @multi_mutex = Mutex.new
    @buffer = Buffer.for connection, table_name
  end

  def row(selector, document)
    buffer.add selector, document
  end

  def multi
    @multi_mutex.synchronize do
      buffer.async = true
      yield self
      buffer.async = false
      buffer.clear
    end
  end
end
