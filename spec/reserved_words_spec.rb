require "spec_helper"
describe Upsert do
  describe "doesn't blow up on reserved words" do
    # collect and uniq reserved words
    reserved_words = ["mysql_reserved.txt", "pg_reserved.txt"].map { |basename|
      File.expand_path("../misc/#{basename}", __FILE__)
    }.map { |path|
      IO.readlines(path)
    }.flatten.map(&:chomp).select(&:present?).uniq

    # make lots of AR models, each of which has 10 columns named after these words
    nasties = []
    reserved_words.each_slice(10) do |words|
      name = "Nasty#{nasties.length}"
      Object.const_set(name, Class.new(ActiveRecord::Base))
      nasty = Object.const_get(name)
      nasty.class_eval do
        self.table_name = name.downcase
        self.primary_key = "fake_primary_key"
      end

      Sequel.migration {
        change do
          db = self
          create_table?(name.downcase) do
            primary_key :fake_primary_key
            words.each do |word|
              String word, limit: 191
            end
          end
        end
      }.apply(DB, :up)
      nasties << [nasty, words]
    end

    describe "reserved words" do
      nasties.each do |nasty, words|
        it "doesn't die on reserved words #{words.join(",")}" do
          upsert = Upsert.new $conn, nasty.table_name
          random = rand(1e3)
          selector = {:fake_primary_key => random, words.first => words.first}
          setter = words[1..-1].each_with_object({}) { |word, memo| memo[word] = word; }
          assert_creates nasty, [selector.merge(setter)] do
            upsert.row selector, setter
          end
        end
      end
    end
  end
end
