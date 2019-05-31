require 'upsert/connection/jdbc'
require 'upsert/connection/sqlite3'

class Upsert
  class Connection
    # @private
    class Java_OrgSqlite_Conn < Connection
      include Jdbc
      include Sqlite3

      # ?
      def quote_ident(k)
        DOUBLE_QUOTE + k.to_s.gsub(DOUBLE_QUOTE, '""') + DOUBLE_QUOTE
      end
    end
  end
end
