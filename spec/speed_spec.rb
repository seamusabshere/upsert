require "spec_helper"
describe Upsert do
  describe "can be speeded up with upserting" do
    describe "compared to native ActiveRecord" do
      it "is faster than new/set/save" do
        assert_faster_than "find + new/set/save", lotsa_records do |records|
          records.each do |selector, setter|
            if pet = Pet.where(selector).first
              pet.update_attributes(setter)
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
      it "is faster than find_or_create + update_attributes" do
        assert_faster_than "find_or_create + update_attributes", lotsa_records do |records|
          dynamic_method = nil
          records.each do |selector, setter|
            Pet.find_or_create_by(selector).update_attributes(setter)
          end
        end
      end
      it "is faster than create + rescue/find/update" do
        assert_faster_than "create + rescue/find/update", lotsa_records do |records|
          records.each do |selector, setter|
            begin
              Pet.create selector.merge(setter), without_protection: true
            rescue
              Pet.find_or_create_by(selector).update_attributes(setter)
            end
          end
        end
      end
    end
  end
end
