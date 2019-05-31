class Upsert
  # @private
  class Row
    if RUBY_VERSION >= "1.9"
      OrderedHash = ::Hash
    else
      begin
        require "orderedhash"
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

      @selector = raw_selector.each_with_object({}) { |(k, v), memo|
        memo[k.to_s] = v
      }

      @hstore_delete_keys = {}
      @setter = raw_setter.each_with_object({}) { |(k, v), memo|
        k = k.to_s
        if v.is_a?(::Hash) && eager_nullify
          v.each do |kk, vv|
            if vv.nil?
              (@hstore_delete_keys[k] ||= []) << kk
            end
          end
        end
        memo[k] = v
      }

      (selector.keys - setter.keys).each do |missing|
        setter[missing] = selector[missing]
      end

      # there is probably a more clever way to incrementally sort these hashes
      @selector = sort_hash selector
      @setter = sort_hash setter
    end

    private

    def sort_hash(original)
      original.keys.sort.each_with_object(OrderedHash.new) do |k, memo|
        memo[k] = original[k]
      end
    end
  end
end
