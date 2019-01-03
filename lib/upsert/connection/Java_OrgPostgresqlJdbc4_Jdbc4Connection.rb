require 'upsert/connection/jdbc'
require 'upsert/connection/postgresql'

class Upsert
  class Connection
    # @private
    class Java_OrgPostgresqlJdbc4_Jdbc4Connection < Connection
      include Jdbc
      include Postgresql

      def quote_ident(k)
        DOUBLE_QUOTE + k.to_s.gsub(DOUBLE_QUOTE, '""') + DOUBLE_QUOTE
      end

      def in_transaction?
        # https://github.com/kares/activerecord-jdbc-adapter/commit/4d6e0e0c52d12b0166810dffc9f898141a23bee6
        ![0, 4].include?(metal.get_transaction_state)
      end
    end
  end
end
