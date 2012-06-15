class Upsert
  module Quoter
    ISO8601_DATE = '%F'

    def quote_value(v)
      case v
      when NilClass
        'NULL'
      when Numeric
        v
      when String
        quote_string v # must be defined by base
      when Upsert::Binary
        quote_binary v.v # must be defined by base
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

    def quote_idents(idents)
      idents.map { |k| quote_ident(k) }.join(',') # must be defined by base
    end

    def quote_values(values)
      values.map { |v| quote_value(v) }.join(',')
    end
    
    def quote_pairs(pairs)
      pairs.map { |k, v| [quote_ident(k),quote_value(v)].join('=') }.join(',')
    end
  end
end
