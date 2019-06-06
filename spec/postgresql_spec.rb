require 'spec_helper'
describe Upsert do
  version = Pet.connection.select_value("SHOW server_version")[0..4].to_f

  let(:upsert) do
    Upsert.new($conn, :pets)
  end

  it "uses the native method if available (#{(UNIQUE_CONSTRAINT && version >= 9.5).inspect})" do
    p = Pet.create(:name => 'Jerry', :tag_number => 5)
    upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
    expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to(
      UNIQUE_CONSTRAINT && version >= 9.5 ? be_truthy : be_falsey
    )
  end
end if ENV['DB'] == 'postgresql'
