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

    def quoted_values_length
      @quoted_values_length ||= pairs.inject(0) { |sum, (_, v)| sum + buffer.quoted_value_length(v) }
    end

    def quoted_values
      buffer.quote_values values
    end

    def values
      pairs.map { |_, v| v }
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
