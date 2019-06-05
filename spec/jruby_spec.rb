require 'spec_helper'
describe Upsert do
  it "works correct with large ints" do
    u = Upsert.new($conn, :pets)
    Pet.create(:name => "Jerry", :big_tag_number => 2)
    u.row({ :name => 'Jerry' }, :big_tag_number => 3599657714)
    Pet.find_by_name('Jerry').big_tag_number.should == 3599657714
  end
end if RUBY_PLATFORM == 'java'
