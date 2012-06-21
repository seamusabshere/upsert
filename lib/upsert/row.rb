class Upsert
  # @private
  class Row
    attr_reader :parent
    attr_reader :selector
    attr_reader :document

    def initialize(parent, selector, document)
      @parent = parent
      @selector = selector
      @document = document
    end

    def columns
      @columns ||= (selector.keys + document.keys).uniq
    end

    def values_sql_bytesize
      @values_sql_bytesize ||= pairs.inject(0) { |sum, (_, v)| sum + parent.quoted_value_bytesize(v) }
    end

    def values_sql
      parent.quote_values pairs.map { |_, v| v }
    end

    def columns_sql
      parent.quote_idents columns
    end

    def where_sql
      parent.quote_pairs selector
    end

    def set_sql
      parent.quote_pairs pairs
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
