class Upsert
  module ActiveRecordUpsert
    def upsert(selector, setter = {})
      ActiveRecord::Base.connection_pool.with_connection do |c|
        upsert = Upsert.new c, table_name
        upsert.row selector, setter
      end
    end

    # records is an array of hashes with keys :selector and :setter
    # # selector and setter are hashes with { :column_name => value }
    # # the values have to be the right type; Upsert doesn't do type coercion like most of ActiveRecord
    # Takes a batch_size option which allows the function to split up the full query into multiple batches
    # Takes an optional block which is called between batches after the full batch (can be useful for logging)
    def batch_upsert(records, options={})
      options.reverse_merge!(
        batch_size: records.count
      )

      ActiveRecord::Base.connection_pool.with_connection do |c|
        until records.empty?
          yield
          this_batch = records.shift(options[:batch_size])
          Upsert.batch(c, table_name) do |up|
            this_batch.each do |record|
              up.row(record[:selector], record[:setter])
            end
          end
        end
        yield
      end
    end
  end
end

ActiveRecord::Base.extend Upsert::ActiveRecordUpsert
