require "spec_helper"
describe Upsert do
  describe "supports multibyte" do
    it "works one-by-one" do
      assert_creates(Pet, [{name: "I♥NY", gender: "périferôl"}]) do
        upsert = Upsert.new $conn, :pets
        upsert.row({name: "I♥NY"}, {gender: "périferôl"})
      end
    end
    it "works serially" do
      assert_creates(Pet, [{name: "I♥NY", gender: "jÚrgen"}]) do
        upsert = Upsert.new $conn, :pets
        upsert.row({name: "I♥NY"}, {gender: "périferôl"})
        upsert.row({name: "I♥NY"}, {gender: "jÚrgen"})
      end
    end
    it "works batch" do
      assert_creates(Pet, [{name: "I♥NY", gender: "jÚrgen"}]) do
        Upsert.batch($conn, :pets) do |upsert|
          upsert.row({name: "I♥NY"}, {gender: "périferôl"})
          upsert.row({name: "I♥NY"}, {gender: "jÚrgen"})
        end
      end
    end
  end
end
