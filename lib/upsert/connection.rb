class Upsert
  # @private
  class Connection
    attr_reader :controller

    def initialize(controller, metal_provider)
      @controller = controller
      @metal_provider = metal_provider
    end

    def metal
      Upsert.metal @metal_provider
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
      when Symbol
        v.to_s
      else
        v
      end
    end

  end
end
