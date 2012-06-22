class Upsert
  # @private
  class Row
    attr_reader :parent
    attr_reader :raw_selector
    attr_reader :selector
    attr_reader :document

    def initialize(parent, raw_selector, raw_document)
      @parent = parent
      @raw_selector = raw_selector
      @selector = raw_selector.inject({}) do |memo, (k, v)|
        memo[parent.quote_ident(k)] = parent.quote_value(v)
        memo
      end
      @document = raw_document.inject({}) do |memo, (k, v)|
        memo[parent.quote_ident(k)] = parent.quote_value(v)
        memo
      end
    end

    def columns
      @columns ||= (selector.keys + document.keys).uniq
    end

    def values_sql_bytesize
      @values_sql_bytesize ||= pairs.inject(0) { |sum, (_, v)| sum + v.to_s.bytesize } + columns.length - 1
    end

    def values_sql
      pairs.map { |_, v| v }.join(',')
    end

    def columns_sql
      pairs.map { |k, _| k }.join(',')
    end

    def where_sql
      selector.map { |k, v| [k, v].join('=') }.join(' AND ')
    end

    def set_sql
      pairs.map { |k, v| [k, v].join('=') }.join(',')
    end

    def pairs
      @pairs ||= columns.map do |k|
        v = if document.has_key?(k)
          # prefer the document so that you can change rows
          document[k]
        else
          selector[k]
        end
        [ k, v ]
      end
    end

    def to_hash
      @to_hash ||= pairs.inject({}) do |memo, (k, v)|
        memo[k] = v
        memo
      end
    end
  end
end
