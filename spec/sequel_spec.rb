require 'spec_helper'
require 'sequel'

describe Upsert do
  describe "Plays nice with Sequel" do
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    case
      when 'postgresql' == config[:adapter]; config[:adapter] = 'postgres'
      when 'sqlite3' == config[:adapter]; config[:adapter] = 'sqlite'
    end

    it "Doesn't explode on connection" do
      expect { DB = Sequel.connect config }.to_not raise_error
    end

    it "Doesn't explode when using DB.pool.hold" do
      DB.pool.hold do |conn|
        expect {
          upsert = Upsert.new(conn, :pets)
          assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
            upsert.row({:name => 'Jerry'}, {:gender => 'male'})
          end
        }.to_not raise_error
      end
    end

    it "Doesn't explode when using DB.synchronize" do
      DB.synchronize do |conn|
        expect {
          upsert = Upsert.new(conn, :pets)
          assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
            upsert.row({:name => 'Jerry'}, {:gender => 'male'})
          end
        }.to_not raise_error
      end
    end
  end
end