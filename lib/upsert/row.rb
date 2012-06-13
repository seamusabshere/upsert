class Upsert
  class Row
    attr_reader :selector
    attr_reader :document
    def initialize(selector, document)
      @selector = selector
      @document = document
    end
    def columns
      @columns ||= (selector.keys+document.keys).uniq
    end
    def pairs
      @pairs ||= columns.map do |k|
        value = if selector.has_key?(k)
          selector[k]
        else
          document[k]
        end
        [ k, value ]
      end
    end
    def inserts
      @inserts ||= pairs.map { |_, v| v }
    end
    def updates
      @updates ||= pairs.reject { |k, _| selector.has_key?(k) }
    end
    def to_hash
      @to_hash ||= pairs.inject({}) do |memo, (k, v)|
        memo[k.to_s] = v
        memo
      end
    end
  end
end
