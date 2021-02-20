require 'spec_helper'
describe Upsert do
  let(:upsert) { Upsert.new(Pet.connection, :pets) }
  describe "timezone support" do
    it "takes times in UTC" do
      time = Time.new.utc
      if ENV['DB'] == 'mysql'
        time = time.change(:usec => 0)
      end
      assert_creates(Pet, [[{:name => 'Jerry'}, {:morning_walk_time => time}]]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end

    it "takes times in local" do
      time = Time.new
      if ENV['DB'] == 'mysql'
        time = time.change(:usec => 0)
      end
      assert_creates(Pet, [[{:name => 'Jerry'}, {:morning_walk_time => time}]]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end

    it "takes datetimes in UTC" do
      time = DateTime.now.new_offset(Rational(0, 24))
      if ENV['DB'] == 'mysql'
        time = time.change(:usec => 0)
      end
      assert_creates(Pet, [[{:name => 'Jerry'}, {:morning_walk_time => time}]]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end

    it "takes datetimes in local" do
      time = DateTime.now
      if ENV['DB'] == 'mysql'
        time = time.change(:usec => 0)
      end
      assert_creates(Pet, [[{:name => 'Jerry'}, {:morning_walk_time => time}]]) do
        upsert.row({:name => 'Jerry'}, {:morning_walk_time => time})
      end
    end

    if ENV['DB'] == 'postgresql'
      it "doesn't die on timestamp without time zone (postgresql)" do
        time = Time.new.utc
        assert_creates(Pet, [[{:name => 'Jerry'}, {:tsntz => time}]]) do
          upsert.row({:name => 'Jerry'}, {:tsntz => time})
        end
      end
    end
  end
end
