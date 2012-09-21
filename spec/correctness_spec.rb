require 'spec_helper'
describe Upsert do
  describe 'clever correctness' do
    it "doesn't confuse selector and setter" do
      p = Pet.new
      p.name = 'Jerry'
      p.tag_number = 5
      p.save!

      # won't change anything because selector is wrong
      u = Upsert.new($conn, :pets)
      selector = {:name => 'Jerry', :tag_number => 6}
      u.row(selector)
      Pet.find_by_name('Jerry').tag_number.should == 5

      # won't change anything because selector is wrong
      u = Upsert.new($conn, :pets)
      selector = {:name => 'Jerry', :tag_number => 10}
      setter = { :tag_number => 5 }
      u.row(selector, setter)
      Pet.find_by_name('Jerry').tag_number.should == 5

      u = Upsert.new($conn, :pets)
      selector = { :name => 'Jerry' }
      setter = { :tag_number => 10 }
      u.row(selector, setter)
      Pet.find_by_name('Jerry').tag_number.should == 10

      u = Upsert.new($conn, :pets)
      selector = { :name => 'Jerry', :tag_number => 10 }
      setter = { :tag_number => 20 }
      u.row(selector, setter)
      Pet.find_by_name('Jerry').tag_number.should == 20
    end
  end
  describe "is just as correct as other ways" do
    describe 'compared to native ActiveRecord' do
      it "is as correct as than new/set/save" do
        assert_same_result lotsa_records do |records|
          records.each do |selector, setter|
            if pet = Pet.where(selector).first
              pet.update_attributes setter, :without_protection => true
            else
              pet = Pet.new
              selector.each do |k, v|
                pet.send "#{k}=", v
              end
              setter.each do |k, v|
                pet.send "#{k}=", v
              end
              pet.save!
            end
          end
        end
      end
      it "is as correct as than find_or_create + update_attributes" do
        assert_same_result lotsa_records do |records|
          dynamic_method = nil
          records.each do |selector, setter|
            dynamic_method ||= "find_or_create_by_#{selector.keys.join('_or_')}"
            pet = Pet.send(dynamic_method, *selector.values)
            pet.update_attributes setter, :without_protection => true
          end
        end
      end
      it "is as correct as than create + rescue/find/update" do
        assert_same_result lotsa_records do |records|
          dynamic_method = nil
          records.each do |selector, setter|
            dynamic_method ||= "find_or_create_by_#{selector.keys.join('_or_')}"
            begin
              Pet.create selector.merge(setter), :without_protection => true
            rescue
              pet = Pet.send(dynamic_method, *selector.values)
              pet.update_attributes setter, :without_protection => true
            end
          end
        end
      end
    end

    if ENV['ADAPTER'] == 'mysql2'
      describe 'compared to activerecord-import' do
        it "is as correct as faking upserts with activerecord-import" do
          assert_same_result lotsa_records do |records|
            columns = nil
            all_values = []
            records.each do |selector, setter|
              columns ||= (selector.keys + setter.keys).uniq
              all_values << columns.map do |k|
                if setter.has_key?(k)
                  # prefer the setter so that you can change rows
                  setter[k]
                else
                  selector[k]
                end
              end
            end
            Pet.import columns, all_values, :timestamps => false, :on_duplicate_key_update => columns
          end
        end
      end
    end
    
  end
end
