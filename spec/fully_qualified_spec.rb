require 'spec_helper'
describe Upsert do
  describe "can work with a fully-qualified name" do
    it "works without a fully qualified name" do
      upsert = Upsert.new $conn, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end

    it "works with a fully qualified name" do
      upsert = Upsert.new $conn, [:upsert_test2, :pets]
      assert_creates(Pet2, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end
  end
end
