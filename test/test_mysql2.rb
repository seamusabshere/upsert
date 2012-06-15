require 'helper'
require 'mysql2'

system %{ mysql -u root -ppassword -e "DROP DATABASE IF EXISTS test_upsert; CREATE DATABASE test_upsert CHARSET utf8" }
ActiveRecord::Base.establish_connection :adapter => 'mysql2', :username => 'root', :password => 'password', :database => 'test_upsert'

describe "upserting on mysql2" do
  before do
    ActiveRecord::Base.connection.drop_table(Pet.table_name) rescue nil
    Pet.auto_upgrade!
    @opened_connections = []
    @connection = new_connection
  end
  after do
    @opened_connections.each { |c| c.close }
  end
  def new_connection
    c = Mysql2::Client.new(:username => 'root', :password => 'password', :database => 'test_upsert')
    @opened_connections << c
    c
  end
  def connection
    @connection
  end

  it_behaves_like 'a database with an upsert trick'

  it_behaves_like 'something that can be speeded up with upserting'

  it_behaves_like 'something that supports binary upserts'

  it_behaves_like "something that supports multibyte"

  it_behaves_like "doesn't mess with timezones"
end
