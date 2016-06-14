require 'spec_helper'
describe Upsert do
  describe "is a database with an upsert trick" do
    describe :row do
      it "works for a single row (base case)" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        end
      end
      it "works for complex selectors" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male', :tag_number => 4}]) do
          upsert.row({:name => 'Jerry', :gender => 'male'}, {:tag_number => 1})
          upsert.row({:name => 'Jerry', :gender => 'male'}, {:tag_number => 4})
        end
      end
      it "doesn't nullify columns that are not included in the selector or setter" do
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male', :tag_number => 4}]) do
          one = Upsert.new $conn, :pets
          one.row({:name => 'Jerry'}, {:gender => 'male'})
          two = Upsert.new $conn, :pets
          two.row({:name => 'Jerry'}, {:tag_number => 4})
        end
      end
      it "works for a single row (not changing anything)" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        end
      end
      it "works for a single row (changing something)" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
          upsert.row({:name => 'Jerry'}, {:gender => 'neutered'})
        end
        expect(Pet.where(:gender => 'male').count).to eq(0)
      end
      it "works for a single row with implicit nulls" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
          upsert.row({:name => 'Inky'}, {})
          upsert.row({:name => 'Inky'}, {})
        end
      end
      it "works for a single row with empty setter" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
          upsert.row(:name => 'Inky')
          upsert.row(:name => 'Inky')
        end
      end
      it "works for a single row with explicit nulls" do
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
          upsert.row({:name => 'Inky'}, {:gender => nil})
          upsert.row({:name => 'Inky'}, {:gender => nil})
        end
      end
      it "works with ids" do
        jerry = Pet.create :name => 'Jerry', :lovability => 1.0
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :lovability => 2.0}]) do
          upsert.row({:id => jerry.id}, :lovability => 2.0)
        end
      end
      it "does not set the created_at and created_on columns on update" do
        task = Task.create :name => 'Clean bathroom'
        created = task.created_at
        upsert = Upsert.new $conn, :tasks
        upsert.row({:id => task.id}, :name => 'Clean kitchen')
        task.reload
        expect(task.created_at).to eql task.created_at
        expect(task.created_on).to eql task.created_on
      end

      it "converts symbol values to string" do
        jerry = Pet.create :name => 'Jerry', :gender => 'female'
        upsert = Upsert.new $conn, :pets
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:id => jerry.id}, :gender => :male)
        end
      end

      it "works for column names with spaces in them" do
        upsert = Upsert.new $conn, :people
        assert_creates(Person, [{:"First Name" => 'Major', :"Last Name" => 'Major'}]) do
          upsert.row({:"First Name" => 'Major'}, :"Last Name" => 'Major')
        end
      end
    end
    describe :batch do
      it "works for multiple rows (base case)" do
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
          Upsert.batch($conn, :pets) do |upsert|
            upsert.row({:name => 'Jerry'}, :gender => 'male')
          end
        end
      end
      it "works for multiple rows (not changing anything)" do
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
          Upsert.batch($conn, :pets) do |upsert|
            upsert.row({:name => 'Jerry'}, :gender => 'male')
            upsert.row({:name => 'Jerry'}, :gender => 'male')
          end
        end
      end
      it "works for multiple rows (changing something)" do
        assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
          Upsert.batch($conn, :pets) do |upsert|
            upsert.row({:name => 'Jerry'}, :gender => 'male')
            upsert.row({:name => 'Jerry'}, :gender => 'neutered')
          end
        end
        expect(Pet.where(:gender => 'male').count).to eq(0)
      end
    end
  end
end
