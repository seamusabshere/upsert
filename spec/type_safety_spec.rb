require "spec_helper"
if ENV["DB"] == "postgresql"
  describe Upsert do
    describe "type safety" do
      it "does not attempt to typecast values" do
        upsert = Upsert.new $conn, :pets
        lambda do
          upsert.row tag_number: ""
        end.should raise_error
      end
    end
  end
end
