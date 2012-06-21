require 'helper'
require 'mysql2'

system %{ mysql -u root -ppassword -e "DROP DATABASE IF EXISTS test_upsert; CREATE DATABASE test_upsert CHARSET utf8" }
ActiveRecord::Base.establish_connection :adapter => 'mysql2', :username => 'root', :password => 'password', :database => 'test_upsert'

require 'upsert/active_record_upsert'

describe Upsert::ActiveRecordUpsert do
  before do
    ActiveRecord::Base.connection.drop_table(Pet.table_name) rescue nil
    Pet.auto_upgrade!
  end

  describe :upsert do
    it "is easy to use" do
      assert_creates(Pet,[{:name => 'Jerry', :good => true}]) do
        Pet.upsert({:name => 'Jerry'}, :good => false)
        Pet.upsert({:name => 'Jerry'}, :good => true)
      end
    end
  end
end
