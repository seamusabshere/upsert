require 'spec_helper'

require 'upsert/active_record_upsert'

describe Upsert do
  describe 'the optional active_record extension' do
    describe :upsert do
      it "is easy to use" do
        assert_creates(Pet,[{:name => 'Jerry', :good => true}]) do
          Pet.upsert({:name => 'Jerry'}, :good => false)
          Pet.upsert({:name => 'Jerry'}, :good => true)
        end
      end
    end
  end
end
