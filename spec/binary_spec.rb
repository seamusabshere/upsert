require 'spec_helper'
describe Upsert do
  describe "supports binary upserts" do
    before do
      @fakes = []
      10.times do
        @fakes << [Faker::Name.name, Faker::Lorem.paragraphs(10).join("\n\n")]
      end
    end
    it "saves binary one by one" do
      @fakes.each do |name, biography|
        zipped_biography = Zlib::Deflate.deflate biography
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => name, :zipped_biography => zipped_biography}]) do
          upsert.row({:name => name}, {:zipped_biography => Upsert.binary(zipped_biography)})
          # binding.pry
        end

        Zlib::Inflate.inflate(Pet.find_by_name(name).zipped_biography).should == biography
      end
    end
  end
end