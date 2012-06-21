shared_examples_for 'is a database with an upsert trick' do
  describe :row do
    it "works for a single row (base case)" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end
    it "works for complex selectors" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male', :tag_number => 4}]) do
        upsert.row({:name => 'Jerry', :gender => 'male'}, {:tag_number => 1})
        upsert.row({:name => 'Jerry', :gender => 'male'}, {:tag_number => 4})
      end
    end
    it "works for a single row (not changing anything)" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end
    it "works for a single row (changing something)" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        upsert.row({:name => 'Jerry'}, {:gender => 'neutered'})
      end
      Pet.where(:gender => 'male').count.must_equal 0
    end

    it "works for a single row with implicit nulls" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
        upsert.row({:name => 'Inky'}, {})
        upsert.row({:name => 'Inky'}, {})
      end
    end
    it "works for a single row with explicit nulls" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
        upsert.row({:name => 'Inky'}, {:gender => nil})
        upsert.row({:name => 'Inky'}, {:gender => nil})
      end
    end
  end
  describe :batch do
    it "works for multiple rows (base case)" do
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        Upsert.batch(connection, :pets) do |upsert|
          upsert.row({:name => 'Jerry'}, :gender => 'male')
        end
      end
    end
    it "works for multiple rows (not changing anything)" do
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        Upsert.batch(connection, :pets) do |upsert|
          upsert.row({:name => 'Jerry'}, :gender => 'male')
          upsert.row({:name => 'Jerry'}, :gender => 'male')
        end
      end
    end
    it "works for multiple rows (changing something)" do
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
        Upsert.batch(connection, :pets) do |upsert|
          upsert.row({:name => 'Jerry'}, :gender => 'male')
          upsert.row({:name => 'Jerry'}, :gender => 'neutered')
        end
      end
      Pet.where(:gender => 'male').count.must_equal 0
    end
  end
end
