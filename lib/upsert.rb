require 'upsert/version'
require 'upsert/buffer'
require 'upsert/quoter'
require 'upsert/row'
require 'upsert/buffer/mysql2_client'
require 'upsert/buffer/pg_connection'
require 'upsert/buffer/sqlite3_database'

class Upsert
  INFINITY = 1.0/0
  SINGLE_QUOTE = %{'}
  DOUBLE_QUOTE = %{"}
  BACKTICK = %{`}
  ISO8601_DATETIME = '%Y-%m-%dT%l:%M:%S%z'
  ISO8601_DATE = '%F'

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
      begin
        buffer.async = true
        yield self
        buffer.clear
      ensure
        buffer.async = nil
      end
    end
  end
end
