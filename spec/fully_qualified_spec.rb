require 'spec_helper'

class FQWeird < ActiveRecord::Base
  self.table_name = "weird."
  col :name, limit: 191 # utf8mb4 in mysql requirement
end


describe Upsert do
  describe "can work with a fully-qualified name" do
    it "works without a fully qualified name" do
      upsert = Upsert.new $conn, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end

    it "works with a fully qualified name" do
      cls = Class.new(Pet)
      cls.table_name = "#{RawConnectionFactory::DATABASE}2.pets"
      cls.auto_upgrade!

      upsert = Upsert.new $conn, [:upsert_test2, :pets]
      assert_creates(cls, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end

    context "with a reserved character" do
      it "works without a fully qualified name" do
        cls = Class.new(Pet)
        cls.class_eval do
          self.table_name = "#{RawConnectionFactory::DATABASE}2.#{$conn.quote_ident('asdf.grr')}"
          reset_model!
        end

        cls.auto_upgrade!

        upsert = Upsert.new $conn, [:upsert_test2, 'asdf.grr']
        assert_creates(cls, [{:name => 'Jerry', :gender => 'male'}]) do
          upsert.row({:name => 'Jerry'}, {:gender => 'male'})
        end
      end
    end
  end
end
