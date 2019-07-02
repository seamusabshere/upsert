require 'spec_helper'

describe Upsert do
  it "works without a fully qualified name" do
    upsert = Upsert.new $conn, :pets
    assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
      upsert.row({:name => 'Jerry'}, {:gender => 'male'})
    end
  end

  it "works with a fully qualified name" do
    table_name = ["#{RawConnectionFactory::DB_NAME}2", :pets]
    cls = clone_ar_class(Pet, table_name)
    upsert = Upsert.new $conn, table_name
    assert_creates(cls, [{:name => 'Jerry', :gender => 'male'}]) do
      upsert.row({:name => 'Jerry'}, {:gender => 'male'})
    end
  end

  if ENV['DB'] == 'postgresql'
    context "with a reserved character" do
      it "works without a fully qualified name" do
        table_name = 'asdf.`grr'
        cls = clone_ar_class(Pet, table_name)
        upsert = Upsert.new $conn, table_name
        assert_creates(cls, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        end
      end

      it "works with a fully qualified name" do
        table_name = ["#{RawConnectionFactory::DB_NAME}2", 'asdf.`grr']
        cls = clone_ar_class(Pet, table_name)
        upsert = Upsert.new $conn, table_name
        assert_creates(cls, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        end
      end
    end
  end
end
