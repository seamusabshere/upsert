require 'upsert/connection/mysql2_client'
require 'upsert/connection/pg_connection'
require 'upsert/connection/sqlite3_database'

class Upsert
  # @private
  class Connection
    attr_reader :parent
    attr_reader :raw_connection

    def initialize(parent, raw_connection)
      @parent = parent
      @raw_connection = raw_connection
    end
    
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
end
