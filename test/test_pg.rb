require 'helper'
require 'pg'

system %{ dropdb test_upsert }
system %{ createdb test_upsert }
ActiveRecord::Base.establish_connection :adapter => 'postgresql', :database => 'test_upsert'

describe "upserting on postgresql" do
  before do
    ActiveRecord::Base.connection.drop_table(Pet.table_name) rescue nil
    Pet.auto_upgrade!
    @opened_connections = []
    @connection = new_connection
  end
  after do
    @opened_connections.each { |c| c.finish }
  end
  def new_connection
    c = PG.connect(:dbname => 'test_upsert')
    @opened_connections << c
    c
  end
  def connection
    @connection
  end

  it_behaves_like 'a database with an upsert trick'

  it_behaves_like 'something that can be speeded up with upserting'

  it_behaves_like 'something that supports binary upserts'

end
