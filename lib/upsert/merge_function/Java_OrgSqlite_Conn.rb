require "upsert/merge_function/sqlite3"

class Upsert
  class MergeFunction
    # @private
    class Java_OrgSqlite_Conn < MergeFunction
      include Sqlite3
    end
  end
end
