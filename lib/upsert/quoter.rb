class Upsert
  module Quoter
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
