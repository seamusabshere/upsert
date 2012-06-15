class Upsert
  module Quoter
    NULL = 'NULL'
    ISO8601_DATETIME = '%Y-%m-%d %H:%M:%S' #FIXME ignores timezones i think
    ISO8601_DATE = '%F'

    # basic

    def quote_value(v)
      case v
      when NilClass
        NULL
      when Numeric
        v
      when String
        self.class.const_get(:QUOTE_VALUE) + escape_string(v) + self.class.const_get(:QUOTE_VALUE)
      when Symbol
        quote_value v.to_s
      when Time, DateTime
        if self.class.const_defined?(:USEC_PRECISION) and self.class.const_get(:USEC_PRECISION)
          quote_value "#{v.strftime(ISO8601_DATETIME)}.#{sprintf("%06d", v.usec)}"
        else
          quote_value v.strftime(ISO8601_DATETIME)
        end
      when Date
        quote_value v.strftime(ISO8601_DATE)
      else
        raise "not sure how to quote #{v.class}: #{v.inspect}"
      end
    end
    
    def quote_ident(k)
      self.class.const_get(:QUOTE_IDENT) + escape_ident(k.to_s) + self.class.const_get(:QUOTE_IDENT)
    end

    # lengths

    def quoted_value_length(v)
      case v
      when NilClass
        NULL.length
      when Numeric
        v.to_s.length
      when String
        # conservative
        v.length * 2 + 2
      when Time, DateTime
        24 + 2
      when Date
        10 + 2
      else
        raise "not sure how to get quoted length of #{v.class}: #{v.inspect}"
      end
    end

    # lists
    
    def quote_idents(idents)
      idents.map { |k| quote_ident(k) }.join(',')
    end

    def quote_values(values)
      values.map { |v| quote_value(v) }.join(',')
    end
    
    def quote_pairs(pairs)
      pairs.map { |k, v| [quote_ident(k),quote_value(v)].join('=') }.join(',')
    end
  end
end
