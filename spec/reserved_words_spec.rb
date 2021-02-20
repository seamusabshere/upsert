require 'spec_helper'
describe Upsert do
  describe "doesn't blow up on reserved words" do
    reserved_words = \
      ['mysql_reserved.txt', 'pg_reserved.txt'].map do |basename|
        File.expand_path("../misc/#{basename}", __FILE__)
      end.map do |path|
        IO.readlines(path)
      end.flatten.map(&:chomp).select(&:present?).uniq

    nasties = \
      reserved_words.each_slice(10).each_with_object([]) do |words, obj|
        name = "Nasty#{obj.length}"
        obj << [name, words]
      end

    # make lots of AR models, each of which has 10 columns named after these words
    describe "reserved words" do
      nasties.each do |name, words|
        it "doesn't die on reserved words #{words.join(',')}" do
          Sequel.migration do
            no_transaction
            change do
              create_table!(name.downcase) do
                primary_key :fake_primary_key
                words.each do |word|
                  String word, limit: 191
                end
              end
            end
          end.apply(DB, :up)

          Object.const_set(name, Class.new(ActiveRecord::Base))
          nasty = Object.const_get(name)
          nasty.class_eval do
            self.table_name = name.downcase
            self.primary_key = "fake_primary_key"
          end

          DB.synchronize do |conn|
            upsert = Upsert.new(conn, nasty.table_name)
            random = rand(1e3)
            selector = { :fake_primary_key => random, words.first => words.first }
            setter = words[1..-1].inject({}) { |memo, word| memo[word] = word; memo }
            assert_creates nasty, [selector.merge(setter)] do
              upsert.row selector, setter
            end
          end
        end
      end
    end
  end
end
