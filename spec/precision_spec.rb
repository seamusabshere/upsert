require "spec_helper"
describe Upsert do
  describe "is precise" do
    it "stores small numbers precisely" do
      small = -0.00000000634943
      upsert = Upsert.new $conn, :pets
      upsert.row({name: "NotJerry"}, lovability: small)
      Pet.first.lovability.should be_within(1e-11).of(small) # ?
    end
  end
end
