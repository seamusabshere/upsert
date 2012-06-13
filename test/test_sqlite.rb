require 'helper'
require 'sqlite3'

db_path = File.expand_path('../../tmp/test.sqlite3', __FILE__)
FileUtils.mkdir_p File.dirname(db_path)
FileUtils.rm_f db_path
ActiveRecord::Base.establish_connection :adapter => 'sqlite3', :database => db_path

describe "upserting on sqlite" do
  before do
    ActiveRecord::Base.connection.drop_table Pet.table_name rescue nil
    Pet.auto_upgrade!
    @connection = new_connection
  end
  def new_connection
    db_path = File.expand_path('../../tmp/test.sqlite3', __FILE__)
    SQLite3::Database.open(db_path)
  end
  def connection
    @connection
  end

  it_behaves_like 'a database with an upsert trick'
end
