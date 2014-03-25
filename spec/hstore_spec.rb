# -*- encoding: utf-8 -*-
require 'spec_helper'
describe Upsert do
  describe 'hstore on pg' do
    require 'pg_hstore'
    Pet.connection.execute 'CREATE EXTENSION HSTORE'
    Pet.connection.execute "ALTER TABLE pets ADD COLUMN crazy HSTORE"
    Pet.connection.execute "ALTER TABLE pets ADD COLUMN cool HSTORE"

    before do
      Pet.delete_all
    end

    it "works for ugly text" do
      upsert = Upsert.new $conn, :pets
      uggy = <<-EOS
{"results":[{"locations":[],"providedLocation":{"location":"3001 STRATTON WAY, MADISON, WI 53719 UNITED STATES"}}],"options":{"ignoreLatLngInput":true,"maxResults":1,"thumbMaps":false},"info":{"copyright":{"text":"© 2012 MapQuest, Inc.","imageUrl":"http://api.mqcdn.com/res/mqlogo.gif","imageAltText":"© 2012 MapQuest, Inc."},"statuscode":0,"messages":[]}}
EOS
      upsert.row({:name => 'Uggy'}, :crazy => {:uggy => uggy})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Uggy'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'uggy' => uggy }
    end

    it "just works" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => nil)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      row['crazy'].should == nil

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => nil)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      row['crazy'].should == nil

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {:whatdat => 'whodat'})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'whatdat' => 'whodat' }

      upsert.row({:name => 'Bill'}, :crazy => {:whatdat => "D'ONOFRIO"})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'whatdat' => "D'ONOFRIO" }

      upsert.row({:name => 'Bill'}, :crazy => {:a => 2})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '2', 'whatdat' => "D'ONOFRIO" }
    end

    it "can nullify entire hstore" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => nil)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      row['crazy'].should == nil
    end

    it "deletes keys that are nil" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => nil)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      row['crazy'].should == nil

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => nil})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == {}

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1, :b => 5})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => nil})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1, :b => 5})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => nil, :b => nil, :c => 12})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'c' => '12' }
    end

    it "takes dangerous keys" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => nil)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      row['crazy'].should == nil

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'foo"bar' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'foo"bar' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => nil})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == {}

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => 1, :b => 5})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'foo"bar' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'foo"bar' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => nil})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => 1, :b => 5})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'foo"bar' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:'foo"bar' => nil, :b => nil, :c => 12})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'c' => '12' }
    end

    it "handles multiple hstores" do
      upsert = Upsert.new $conn, :pets
      upsert.row({:name => 'Bill'}, :crazy => {:a => 1, :b => 9}, :cool => {:c => 12, :d => 19})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'b' => '9' }
      cool = PgHstore.parse row['cool']
      cool.should == { 'c' => '12', 'd' => '19' }
    end

    it "can deletes keys from multiple hstores at once" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1}, :cool => {5 => 9})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }
      cool = PgHstore.parse row['cool']
      cool.should == { '5' => '9' }

      # NOOP
      upsert.row({:name => 'Bill'}, :crazy => {}, :cool => {})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1' }
      cool = PgHstore.parse row['cool']
      cool.should == { '5' => '9' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => nil}, :cool => {13 => 17})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == {}
      cool = PgHstore.parse row['cool']
      cool.should == { '5' => '9', '13' => '17' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1, :b => 5})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'b' => '5' }

      upsert.row({:name => 'Bill'}, :crazy => {:b => nil}, :cool => {5 => nil})
      row = Pet.connection.select_one(%{SELECT crazy, cool FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == {'a' => '1'}
      cool = PgHstore.parse row['cool']
      cool.should == {'13' => '17' }
    end

    it "deletes keys whether new or existing record" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, :crazy => {:z => 1, :x => nil})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'z' => '1' }

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'z' => '1' }
    end

    it "can turn off eager nullify" do
      upsert = Upsert.new $conn, :pets

      upsert.row({:name => 'Bill'}, {:crazy => {:z => 1, :x => nil}}, :eager_nullify => false)
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'z' => '1', 'x' => nil }

      upsert.row({:name => 'Bill'}, :crazy => {:a => 1})
      row = Pet.connection.select_one(%{SELECT crazy FROM pets WHERE name = 'Bill'})
      crazy = PgHstore.parse row['crazy']
      crazy.should == { 'a' => '1', 'z' => '1', 'x' => nil}
    end

  end
end if ENV['DB'] == 'postgresql'
