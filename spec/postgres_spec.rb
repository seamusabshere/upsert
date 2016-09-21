require 'spec_helper'
describe Upsert do
  let(:version) do
    version = Pet.connection.select_value("SHOW server_version")[0..2].split('.').join('').to_i
  end

  let(:upsert) do
    Upsert.new($conn, :pets)
  end

  it "uses the native method if available" do
    p = Pet.create(:name => 'Jerry', :tag_number => 5)
    upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
    expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to(
      version >= 95 ? be_truthy : be_falsey
    )
  end

  it "doesn't use upsert if a constraint isn't available" do
    p = Pet.create(:name => 'Jerry', :tag_number => 6)
    upsert.row({ :name => 'Jerry', :tag_number => 6 }, :gender => 'fish')
    expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to be_falsey
  end
end if ENV['DB'] == 'postgresql'
