require 'upsert/connection/jdbc'

class Upsert
  class Connection
    # @private
    class Java_OrgPostgresqlJdbc4_Jdbc4Connection < Connection
      include Jdbc
      include Postgresql

      def quote_ident(k)
        DOUBLE_QUOTE + k.to_s.gsub(DOUBLE_QUOTE, '""') + DOUBLE_QUOTE
      end
    end
  end
end
