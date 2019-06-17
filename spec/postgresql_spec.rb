require 'spec_helper'
require 'upsert/merge_function/postgresql'

describe Upsert do
  version = 'postgresql' == ENV['DB'] ? Upsert::MergeFunction::Postgresql.extract_version(
    Pet.connection.select_value("SHOW server_version")
  ) : 0

  let(:upsert) do
    Upsert.new($conn, :pets)
  end

  it "uses the native method if available (#{(UNIQUE_CONSTRAINT && version >= 90500).inspect})" do
    p = Pet.create(:name => 'Jerry', :tag_number => 5)
    upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
    expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to(
      UNIQUE_CONSTRAINT && version >= 90500 ? be_truthy : be_falsey
    )
  end
end if ENV['DB'] == 'postgresql'
