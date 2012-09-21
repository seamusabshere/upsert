require 'upsert/row/mysql2_client'
require 'upsert/row/pg_connection'
require 'upsert/row/sqlite3_database'

class Upsert
  # @private
  class Row
    if RUBY_VERSION >= '1.9'
      OrderedHash = ::Hash
    else
      begin
        require 'orderedhash'
      rescue LoadError
        raise LoadError, "[upsert] If you're using upsert on Ruby 1.8, you need to add 'orderedhash' to your Gemfile."
      end
      OrderedHash = ::OrderedHash
    end

    attr_reader :selector
    attr_reader :setter

    def initialize(parent, raw_selector, raw_setter)
      conn = parent.connection
      cell_class = parent.cell_class

      @selector = raw_selector.inject({}) do |memo, (k, v)|
        memo[k.to_s] = cell_class.new(conn, k, v)
        memo
      end

      @setter = raw_setter.inject({}) do |memo, (k, v)|
        memo[k.to_s] = cell_class.new(conn, k, v)
        memo
      end

      (selector.keys - setter.keys).each do |missing|
        setter[missing] = selector[missing]
      end

      @selector = sort_hash selector
      @setter = sort_hash setter
    end

    private

    def sort_hash(original)
      original.keys.sort.inject(OrderedHash.new) do |memo, k|
        memo[k] = original[k]
        memo
      end
    end
  end
end
