require 'spec_helper'
describe Upsert do
  describe "type safety" do
    it "does not attempt to typecast values" do
      error_class = RUBY_PLATFORM == 'java' ? org.postgresql.util.PSQLException : PG::InvalidTextRepresentation
      upsert = Upsert.new $conn, :pets
      lambda do
        upsert.row :tag_number => ''
      end.should raise_error error_class
    end
  end
end if ENV['DB'] == 'postgresql'
