class Upsert
  # @private
  class Row
    if RUBY_VERSION >= '1.9'
      OrderedHash = ::Hash
    else
      begin
        require 'orderedhash'
      rescue LoadError
        raise LoadError, "[upsert] If you're using upsert on Ruby 1.8, you need to add 'orderedhash' to your Gemfile."
      end
      OrderedHash = ::OrderedHash
    end

    attr_reader :selector
    attr_reader :setter


    def initialize(controller, raw_selector, raw_setter)
      connection = controller.connection
      cell_class = controller.cell_class

      @selector = raw_selector.inject({}) do |memo, (k, v)|
        memo[k.to_s] = cell_class.new(connection, k, v)
        memo
      end

      @setter = raw_setter.inject({}) do |memo, (k, v)|
        memo[k.to_s] = cell_class.new(connection, k, v)
        memo
      end

      (selector.keys - setter.keys).each do |missing|
        setter[missing] = selector[missing]
      end

      # there is probably a more clever way to incrementally sort these hashes
      @selector = sort_hash selector
      @setter = sort_hash setter
    end

    private

    def sort_hash(original)
      original.keys.sort.inject(OrderedHash.new) do |memo, k|
        memo[k] = original[k]
        memo
      end
    end
  end
end
