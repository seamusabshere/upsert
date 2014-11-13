require 'spec_helper'
describe Upsert do
  describe "is thread-safe" do
    it "for one-by-one use once function has been created" do
      # first make sure to create the function separately (this is not thread-safe on its own)
      ActiveRecord::Base.connection_pool.with_connection do |connection|
        upsert = Upsert.new(connection, :pets, assume_function_exists: false)
        upsert.row({name: 'xxx'}, gender: "blah")
      end
      
      # then get a failure because of connection reuse
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        ts = []
        10.times do
          ts << Thread.new do
            ActiveRecord::Base.connection_pool.with_connection do |connection|
              upsert = Upsert.new(connection, :pets, assume_function_exists: true)
              upsert.row({:name => 'Jerry'}, :gender => 'male')
              upsert.row({:name => 'Jerry'}, :gender => 'neutered')
            end
          end
        end
        ts.each { |t| t.join }
      end
    end
    
    it "for function creation" # to be implemented, but need to delete existing functions first!

    it "is safe to use batch" do
      pending "temporarily disable to underline the main problem"
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        Upsert.batch($conn, :pets) do |upsert|
          ts = []
          10.times do
            ts << Thread.new do
              sleep 0.2
              upsert.row({:name => 'Jerry'}, :gender => 'male')
              upsert.row({:name => 'Jerry'}, :gender => 'neutered')
            end
            ts.each { |t| t.join }
          end
        end
      end
    end
  end
end