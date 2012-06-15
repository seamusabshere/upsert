shared_examples_for 'supports binary upserts' do
  describe 'binary' do
    before do
      @fakes = []
      10.times do
        @fakes << [Faker::Name.name, Faker::Lorem.paragraphs(10).join("\n\n")]
      end
    end
    it "saves binary one by one" do
      @fakes.each do |name, biography|
        zipped_biography = Zlib::Deflate.deflate biography
        upsert = Upsert.new connection, :pets
        assert_creates(Pet, [{:name => name, :zipped_biography => zipped_biography}]) do
          upsert.row({:name => name}, {:zipped_biography => Upsert.binary(zipped_biography)})
        end
        Zlib::Inflate.inflate(Pet.find(name).zipped_biography).must_equal biography
      end
    end
  end
end
