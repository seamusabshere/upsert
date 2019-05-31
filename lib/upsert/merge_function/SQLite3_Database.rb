require "upsert/merge_function/sqlite3"

class Upsert
  class MergeFunction
    # @private
    class SQLite3_Database < MergeFunction
      include Sqlite3
    end
  end
end
