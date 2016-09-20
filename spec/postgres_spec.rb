require 'spec_helper'
describe Upsert do
  let(:version) do
    version = Pet.connection.select_value("SHOW server_version")[0..2].split('.').join('').to_i
  end

  context 'when native upsert available' do
    before(:each) { skip "Not postgres >= 9.5" unless version >= 95 }

    let(:upsert) do
      Upsert.new($conn, :pets)
    end

    it "uses upsert if a constraint is available" do
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_truthy
    end

    it "doesn't use upsert if a constraint isn't available" do
      p = Pet.create(:name => 'Jerry', :tag_number => 6)
      upsert.row({ :name => 'Jerry', :tag_number => 6 }, :gender => 'fish')
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end

    it "doesn't use upsert if you specify not to" do
      upsert = Upsert.new($conn, :pets, disable_native: true)
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end
  end

  context 'when native upsert not availble' do
    before(:each) { skip "Not postgres <= 9.4" unless version <= 94 }

    let(:upsert) do
      Upsert.new($conn, :pets)
    end

    it "doesn't use upsert if a constraint is available" do
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end

    it "doesn't use upsert if a constraint isn't available" do
      p = Pet.create(:name => 'Jerry', :tag_number => 6)
      upsert.row({ :name => 'Jerry', :tag_number => 6 }, :gender => 'fish')
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end

    it "doesn't use upsert if you specify not to" do
      upsert = Upsert.new($conn, :pets, disable_native: true)
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end

    it "doesn't break if you use disable_native: false" do
      upsert = Upsert.new($conn, :pets, disable_native: false)
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.instance_variable_get(:@use_upsert)).to be_falsey
    end
  end
end if ENV['DB'] == 'postgresql'
