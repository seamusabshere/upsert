class Upsert
  class Row
    # @private
    class Mysql2_Client < Row
      attr_reader :original_setter_keys

      def initialize(parent, raw_selector, raw_setter)
        super
        @original_setter_keys = raw_setter.keys.map(&:to_s)
      end

      def quoted_setter_values
        @quoted_setter_values ||= setter.values.map(&:quoted_value)
      end

      def values_sql_bytesize
        @values_sql_bytesize ||= quoted_setter_values.inject(0) { |sum, quoted_value| sum + quoted_value.to_s.bytesize } + setter.length - 1
      end
    end
  end
end
