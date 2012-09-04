require 'upsert/buffer/mysql2_client'
require 'upsert/buffer/pg_connection'
require 'upsert/buffer/sqlite3_database'

class Upsert
  # @private
  class Buffer
    attr_reader :parent
    attr_reader :rows

    def initialize(parent)
      @parent = parent
      @rows = []
    end

    def <<(row)
      rows << row
      ready
    end

    def async?
      !!@async
    end

    def async!
      @async = true
    end

    def sync!
      @async = false
      until rows.empty?
        ready
      end
    end
  end
end
