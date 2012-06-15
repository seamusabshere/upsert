class Upsert
  class Row
    attr_reader :buffer
    attr_reader :selector
    attr_reader :document

    def initialize(buffer, selector, document)
      @buffer = buffer
      @selector = selector
      @document = document
    end

    def columns
      @columns ||= (selector.keys + document.keys).uniq
    end

    def values_sql_length
      @values_sql_length ||= pairs.inject(0) { |sum, (_, v)| sum + buffer.quoted_value_length(v) }
    end

    def values_sql
      buffer.quote_values pairs.map { |_, v| v }
    end

    def columns_sql
      buffer.quote_idents columns
    end

    def where_sql
      buffer.quote_pairs selector
    end

    def set_sql
      buffer.quote_pairs pairs
    end

    def pairs
      @pairs ||= columns.map do |k|
        value = if document.has_key?(k)
          # prefer the document so that you can change rows
          document[k]
        else
          selector[k]
        end
        [ k, value ]
      end
    end

    def to_hash
      @to_hash ||= pairs.inject({}) do |memo, (k, v)|
        memo[k.to_s] = v
        memo
      end
    end
  end
end
