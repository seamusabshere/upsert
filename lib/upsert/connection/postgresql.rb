class Upsert
  class Connection
    # @private
    module Postgresql
      def bind_value(v)
        case v
        when Hash
          # you must require 'hstore' from the 'pg-hstore' gem yourself
          ::HStore.dump(v)[1...-1]
        else
          super
        end
      end
    end
  end
end
