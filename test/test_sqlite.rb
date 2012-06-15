require 'helper'
require 'sqlite3'

db_path = File.expand_path('../../tmp/test.sqlite3', __FILE__)
FileUtils.mkdir_p File.dirname(db_path)
FileUtils.rm_f db_path
ActiveRecord::Base.establish_connection :adapter => 'sqlite3', :database => db_path

describe "upserting on sqlite" do
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
    c = SQLite3::Database.open(File.expand_path('../../tmp/test.sqlite3', __FILE__))
    @opened_connections << c
    c
  end
  def connection
    @connection
  end

  it_behaves_like 'a database with an upsert trick'

  it_behaves_like 'something that can be speeded up with upserting'

  it_behaves_like "something that supports multibyte"

  it_behaves_like "doesn't mess with timezones"

  # FIXME not supported because it requires using bind variables, etc.
  # it_behaves_like 'something that supports binary upserts'
end
