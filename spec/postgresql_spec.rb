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
      table_name = ["#{RawConnectionFactory::DB_NAME}2", :pets2]
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

  describe "array escaping" do
    let(:upsert) do
      Upsert.new($conn, :posts)
    end

    before(:all) do
      Sequel.migration do
        change do
          db = self
          create_table?(:posts) do
            primary_key :id
            String :name
            column :tags, "text[]"
          end
        end
      end.apply(DB, :up)

      Object.const_set("Post", Class.new(ActiveRecord::Base))
    end

    [
      %w[1 2 3],
      %w[can't stop won't stop],
      %w["''" '""' '\\],
      ["[]", "{}", "\\\\", "()"],
      %w[*& *&^ $%IUBS (&^ ) ()*& // \\ \\\\ (*&^JN) (*HNCSD) ~!!!`` {} } { ( )],
      %w[\\ \\\\ \\\\\\ \\\\\\\\ \\'\\'\'\\\'" \\'\\"\''\""],
    ].each do |arr|
      it "properly upserts array of: #{arr}" do
        upsert.row({name: "same-name"}, tags: arr)
        expect(Post.first.tags).to eq(arr)
      end
    end
  end
end if ENV['DB'] == 'postgresql'
