require 'zlib'
require 'upsert/version'

class Upsert
  # @private
  class MergeFunction
    MAX_NAME_LENGTH = 62
    NAME_PREFIX = "upsert#{Upsert::VERSION.gsub('.', '_')}"

    class << self
      def execute(controller, row)
        merge_function = lookup controller, row
        merge_function.execute row
      end

      def unique_name(table_name, selector_keys, setter_keys)
        parts = [
          NAME_PREFIX,
          table_name,
          'SEL',
          selector_keys.join('_A_'),
          'SET',
          setter_keys.join('_A_')
        ].join('_')
        if parts.length > MAX_NAME_LENGTH
          # maybe i should md5 instead
          crc32 = Zlib.crc32(parts).to_s
          [ parts[0..MAX_NAME_LENGTH-10], crc32 ].join
        else
          parts
        end
      end

      def lookup(controller, row)
        selector_keys = row.selector.keys
        setter_keys = row.setter.keys
        key = [controller.table_name, selector_keys, setter_keys]
        @lookup[key] || @lookup_mutex.synchronize do
          @lookup[key] ||= new(controller, selector_keys, setter_keys, controller.assume_function_exists?)
        end
      end
    end

    @lookup = {}
    @lookup_mutex = Mutex.new

    attr_reader :controller
    attr_reader :selector_keys
    attr_reader :setter_keys

    def initialize(controller, selector_keys, setter_keys, assume_function_exists)
      @controller = controller
      @selector_keys = selector_keys
      @setter_keys = setter_keys
      validate!
      create! unless assume_function_exists
    end

    def name
      @name ||= MergeFunction.unique_name table_name, selector_keys, setter_keys
    end

    def connection
      controller.connection
    end

    def table_name
      controller.table_name
    end

    def quoted_table_name
      controller.quoted_table_name
    end

    def column_definitions
      controller.column_definitions
    end

    private

    def validate!
      possible = column_definitions.map(&:name)
      invalid = (setter_keys + selector_keys).uniq - possible
      if invalid.any?
        raise ArgumentError, "[Upsert] Invalid column(s): #{invalid.map(&:inspect).join(', ')}"
      end
    end
  end
end
