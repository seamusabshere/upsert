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
