require 'spec_helper'
describe Upsert do
  Thread.abort_on_exception = true
  describe "is thread-safe" do
    it "is safe to use one-by-one" do
      upsert = Upsert.new $conn, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        ts = []
        10.times do
          ts << Thread.new do
            ActiveRecord::Base.connection_pool.with_connection do |conn|
              sleep 0.2
              upsert.row({:name => 'Jerry'}, :gender => 'male')
              upsert.row({:name => 'Jerry'}, :gender => 'neutered')
            end
          end
        end
        ts.each { |t| t.join(3) }
      end
    end
    it "is safe to use batch" do
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        Upsert.batch($conn, :pets) do |upsert|
          ts = []
          10.times do
            ts << Thread.new do
              ActiveRecord::Base.connection_pool.with_connection do |conn|
                sleep 0.2
                upsert.row({:name => 'Jerry'}, :gender => 'male')
                upsert.row({:name => 'Jerry'}, :gender => 'neutered')
              end
            end
          end
          ts.each { |t| t.join(3) }
        end
      end
    end

    it "is safe to use with the entire block inside the thread" do
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        ts = []
        10.times do
          ts << Thread.new do
            ActiveRecord::Base.connection_pool.with_connection do |conn|
              sleep 0.2
              Upsert.batch(conn, :pets) do |upsert|
                upsert.row({:name => 'Jerry'}, :gender => 'male')
                upsert.row({:name => 'Jerry'}, :gender => 'neutered')
              end
            end
          end
        end
        ts.each { |t| t.join(3) }
      end
    end
  end
end
