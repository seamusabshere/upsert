class Upsert
  module ActiveRecordUpsert
    def upsert(selector, setter = {})
      ActiveRecord::Base.connection_pool.with_connection do |c|
        upsert = Upsert.new c, table_name
        upsert.row selector, setter
      end
    end
  end
end

ActiveRecord::Base.extend Upsert::ActiveRecordUpsert
