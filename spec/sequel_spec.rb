require 'spec_helper'
require 'sequel'

describe Upsert do
  describe "Plays nice with Sequel" do
    config = ActiveRecord::Base.connection.instance_variable_get(:@config)
    config[:adapter] = case config[:adapter]
                       when 'postgresql' then 'postgres'
                       else config[:adapter]
                       end

    let(:db) do
      params = if RUBY_PLATFORM == 'java'
                 RawConnectionFactory::CONFIG
               else
                 config.slice(:adapter, :host, :database, :username, :password).merge(:user => (config[:user] || config[:username]))
               end
      Sequel.connect(params)
    end

    it "Doesn't explode on connection" do
      expect { db }.to_not raise_error
    end

    it "Doesn't explode when using DB.pool.hold" do
      db.pool.hold do |conn|
        expect {
          upsert = Upsert.new(conn, :pets)
          assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
            upsert.row({:name => 'Jerry'}, {:gender => 'male'})
          end
        }.to_not raise_error
      end
    end

    it "Doesn't explode when using DB.synchronize" do
      db.synchronize do |conn|
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
