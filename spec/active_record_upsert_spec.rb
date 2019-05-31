require "spec_helper"

require "upsert/active_record_upsert"

describe Upsert do
  describe "the optional active_record extension" do
    describe :upsert do
      it "is easy to use" do
        assert_creates(Pet, [{name: "Jerry", good: true}]) do
          Pet.upsert({name: "Jerry"}, good: false)
          Pet.upsert({name: "Jerry"}, good: true)
        end
      end

      it "doesn't fail inside a transaction" do
        Upsert.clear_database_functions(Pet.connection)
        expect {
          Pet.transaction do
            Pet.upsert({name: "Simba"}, good: true)
          end
        }.to_not raise_error
        expect(Pet.first.name).to eq("Simba")
      end
    end
  end
end
