class Upsert
  # @private
  class Row
    Cell = Struct.new(:quoted_key, :quoted_value)

    attr_reader :selector
    attr_reader :document

    def initialize(parent, raw_selector, raw_document)
      c = parent.connection
      @selector = raw_selector.inject({}) do |memo, (k, v)|
        memo[k.to_s] = Cell.new(c.quote_ident(k), c.quote_value(v))
        memo
      end
      @document = raw_document.inject({}) do |memo, (k, v)|
        memo[k.to_s] = Cell.new(c.quote_ident(k), c.quote_value(v))
        memo
      end
    end

    def columns
      @columns ||= (selector.keys + document.keys).uniq
    end

    def values_sql_bytesize
      @values_sql_bytesize ||= quoted_pairs.inject(0) { |sum, (_, v)| sum + v.to_s.bytesize } + columns.length - 1
    end

    def values_sql
      quoted_pairs.map { |_, v| v }.join(',')
    end

    def columns_sql
      quoted_pairs.map { |k, _| k }.join(',')
    end

    def where_sql
      selector.map { |_, cell| [cell.quoted_key, cell.quoted_value].join('=') }.join(' AND ')
    end

    def set_sql
      quoted_pairs.map { |k, v| [k, v].join('=') }.join(',')
    end

    def quoted_value(k)
      if c = cell(k)
        c.quoted_value
      end
    end

    def quoted_pairs
      @quoted_pairs ||= columns.map do |k|
        c = cell k
        [ c.quoted_key, c.quoted_value ]
      end
    end

    private

    def cell(k)
      if document.has_key?(k)
        # prefer the document so that you can change rows
        document[k]
      else
        selector[k]
      end
    end
  end
end
