require 'spec_helper'
describe Upsert do
  describe "type safety" do
    it "does not attempt to typecast values" do
      upsert = Upsert.new $conn, :pets
      expect do
        upsert.row :tag_number => ''
      end.to raise_error PG::InvalidTextRepresentation
    end
  end
end if ENV['DB'] == 'postgresql'
