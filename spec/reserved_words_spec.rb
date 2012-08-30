require 'spec_helper'
describe Upsert do
  describe "doesn't blow up on reserved words" do
    # collect and uniq reserved words
    reserved_words = ['mysql_reserved.txt', 'pg_reserved.txt'].map do |basename|
      File.expand_path("../misc/#{basename}", __FILE__)
    end.map do |path|
      IO.readlines(path)
    end.flatten.map(&:chomp).select(&:present?).uniq
  
    # make lots of AR models, each of which has 10 columns named after these words
    nasties = []
    reserved_words.each_slice(10) do |words|
      eval %{
        class Nasty#{nasties.length} < ActiveRecord::Base
        end
      }
      nasty = Object.const_get("Nasty#{nasties.length}")
      nasty.class_eval do
        self.primary_key = 'fake_primary_key'
        col :fake_primary_key
        words.each do |word|
          col word
        end
      end
      nasties << [ nasty, words ]
    end
    nasties.each do |nasty, _|
      nasty.auto_upgrade!
    end
  
    describe "reserved words" do
      nasties.each do |nasty, words|
        it "doesn't die on reserved words #{words.join(',')}" do
          upsert = Upsert.new $conn, nasty.table_name
          random = rand(1e3).to_s
          selector = { :fake_primary_key => random, words.first => words.first }
          document = words[1..-1].inject({}) { |memo, word| memo[word] = word; memo }
          assert_creates nasty, [selector.merge(document)] do
            upsert.row selector, document
          end
        end
      end
    end
  end
end