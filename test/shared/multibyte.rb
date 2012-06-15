# -*- encoding: utf-8 -*-
shared_examples_for "something that supports multibyte" do
  describe :multibyte do
    it "works one-by-one" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'I♥NY', :gender => 'périferôl'}]) do
        upsert.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
      end
    end
    it "works serially" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'I♥NY', :gender => 'jÚrgen'}]) do
        upsert.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
        upsert.row({:name => 'I♥NY'}, {:gender => 'jÚrgen'})
      end
    end
    it "works multi" do
      assert_creates(Pet, [{:name => 'I♥NY', :gender => 'jÚrgen'}]) do
        Upsert.new(connection, :pets).multi do |xxx|
          xxx.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
          xxx.row({:name => 'I♥NY'}, {:gender => 'jÚrgen'})
        end
      end
    end
  end
end
