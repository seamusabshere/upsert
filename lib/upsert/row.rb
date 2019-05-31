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
    attr_reader :hstore_delete_keys

    def initialize(raw_selector, raw_setter, options)
      eager_nullify = (options.nil? || options.fetch(:eager_nullify, true))

      @selector = raw_selector.inject({}) do |memo, (k, v)|
        memo[k.to_s] = v
        memo
      end

      @hstore_delete_keys = {}
      @setter = raw_setter.inject({}) do |memo, (k, v)|
        k = k.to_s
        if v.is_a?(::Hash) and eager_nullify
          v.each do |kk, vv|
            if vv.nil?
              (@hstore_delete_keys[k] ||= []) << kk
            end
          end
        end
        memo[k] = v
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
