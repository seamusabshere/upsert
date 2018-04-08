require 'spec_helper'
describe Upsert do
  version = Pet.connection.select_value("SHOW server_version")[0..2].split('.').join('').to_i

  let(:upsert) do
    Upsert.new($conn, :pets)
  end

  it "uses the native method if available (#{(UNIQUE_CONSTRAINT && version >= 95).inspect})" do
    p = Pet.create(:name => 'Jerry', :tag_number => 5)
    upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
    expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to(
      UNIQUE_CONSTRAINT && version >= 95 ? be_truthy : be_falsey
    )
  end

  if version >= 95 && UNIQUE_CONSTRAINT
    it "works with a schema" do
      table_name = ["#{RawConnectionFactory::DATABASE}2", :pets2]
      cls = clone_ar_class(Pet, table_name)
      upsert = Upsert.new $conn, table_name
      upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to be_truthy
    end

    it "checks the correct table for a unique constraint" do
      Pet.connection.execute("CREATE SCHEMA IF NOT EXISTS unique_constraint_test")
      Pet.connection.execute("CREATE TABLE unique_constraint_test.pets (LIKE public.pets INCLUDING ALL)")
      Pet.connection.execute("SET search_path TO unique_constraint_test")
      $conn.exec("SET search_path TO unique_constraint_test")
      Pet.connection.execute("DROP INDEX unique_constraint_test.pets_name_idx")
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to be_falsey
      Pet.connection.execute("SET search_path TO public")
      $conn.exec("SET search_path TO public")
      Pet.connection.execute("DROP SCHEMA unique_constraint_test CASCADE")
    end
  end
end if ENV['DB'] == 'postgresql'
