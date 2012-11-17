class Upsert
  # @private
  class Connection
    attr_reader :controller
    attr_reader :metal

    def initialize(controller, metal)
      @controller = controller
      @metal = metal
    end

    def convert_binary(bind_values)
      bind_values.map do |v|
        case v
        when Upsert::Binary
          binary v
        else
          v
        end
      end
    end

    def bind_value(v)
      case v
      when Time, DateTime
        Upsert.utc_iso8601 v
      when Date
        v.strftime ISO8601_DATE
      else
        v
      end
    end

  end
end
