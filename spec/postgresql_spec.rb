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

  if version >= 90500 && UNIQUE_CONSTRAINT
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

      if RUBY_PLATFORM == "java"
        $conn.nativeSQL("SET search_path TO unique_constraint_test")
        $conn.setSchema("unique_constraint_test")
      else
        $conn.exec("SET search_path TO unique_constraint_test")
      end

      Pet.connection.execute("DROP INDEX unique_constraint_test.pets_name_idx")
      Pet.connection.execute("ALTER TABLE unique_constraint_test.pets DROP CONSTRAINT IF EXISTS unique_name")
      p = Pet.create(:name => 'Jerry', :tag_number => 5)
      upsert.row({ :name => 'Jerry'}, :tag_number => 6 )
      expect(upsert.instance_variable_get(:@merge_function_cache).values.first.use_pg_native?).to be_falsey
      Pet.connection.execute("SET search_path TO public")

      if RUBY_PLATFORM == "java"
        $conn.nativeSQL("SET search_path TO public")
        $conn.setSchema("public")
      else
        $conn.exec("SET search_path TO public")
      end

      Pet.connection.execute("DROP SCHEMA unique_constraint_test CASCADE")
    end
  end
end if ENV['DB'] == 'postgresql'
