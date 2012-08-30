require 'spec_helper'
describe Upsert do
  describe "doesn't mess with timezones" do
    before do
      @old_default_tz = ActiveRecord::Base.default_timezone
    end
    after do
      ActiveRecord::Base.default_timezone = @old_default_tz
    end
  
    it "deals fine with UTC" do
      ActiveRecord::Base.default_timezone = :utc
      time = Time.now.utc
      upsert = Upsert.new $conn, :pets
      assert_creates(Pet, [{:name => 'Jerry', :morning_walk_time => time}]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end
    it "won't mess with UTC" do
      ActiveRecord::Base.default_timezone = :local
      time = Time.now
      upsert = Upsert.new $conn, :pets
      assert_creates(Pet, [{:name => 'Jerry', :morning_walk_time => time}]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end
  end
end