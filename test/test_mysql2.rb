require 'helper'
require 'mysql2'

system %{ mysql -u root -ppassword -e "DROP DATABASE IF EXISTS test_upsert; CREATE DATABASE test_upsert CHARSET utf8" }
ActiveRecord::Base.establish_connection :adapter => 'mysql2', :username => 'root', :password => 'password', :database => 'test_upsert'

describe Upsert::Mysql2_Client do
  before do
    @opened_connections = []
    ActiveRecord::Base.connection.drop_table(Pet.table_name) rescue nil
    Pet.auto_upgrade!
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

  it_also 'is a database with an upsert trick'

  it_also 'is just as correct as other ways'

  it_also 'can be speeded up with upserting'

  it_also 'supports binary upserts'

  it_also 'supports multibyte'

  it_also 'is thread-safe'

  it_also "doesn't mess with timezones"

  it_also "doesn't blow up on reserved words"

  describe '#sql_bytesize' do
    def assert_exact(selector_proc, document_proc, show = false)
      upsert = Upsert.new connection, :pets
      0.upto(256) do |i|
        upsert.rows << Upsert::Row.new(upsert, selector_proc.call(i), document_proc.call(i))
        i.upto(upsert.rows.length) do |take|
          expected_sql = upsert.sql(take)
          actual = upsert.sql_bytesize(take)
          if show and actual != expected_sql.bytesize
            $stderr.puts
            $stderr.puts "Expected: #{expected_sql.bytesize}"
            $stderr.puts "Actual: #{actual}"
            $stderr.puts expected_sql
          end
          actual.must_equal expected_sql.bytesize
        end
      end
    end
    def rand_string(length)
      # http://www.dzone.com/snippets/generate-random-string-letters
      # Array.new(length) { (rand(122-97) + 97).chr }.join
      if RUBY_VERSION >= '1.9'
        Array.new(length) { rand(512).chr(Encoding::UTF_8) }.join
      else
        Array.new(length) { rand(512) }.pack('C*')
      end
    end
    it "is exact as selector length changes" do
      selector_proc = proc do |i|
        { :name => rand_string(i) }
      end
      document_proc = proc do |i|
        {}
      end
      assert_exact selector_proc, document_proc
    end
    it "is exact as value length changes" do
      selector_proc = proc do |i|
        { :name => 'Jerry' }
      end
      document_proc = proc do |i|
        { :spiel => rand_string(i) }
      end
      assert_exact selector_proc, document_proc
    end
    it "is exact as both selector and value length change" do
      selector_proc = proc do |i|
        { :name => rand_string(i) }
      end
      document_proc = proc do |i|
        { :spiel => rand_string(i) }
      end
      assert_exact selector_proc, document_proc
    end
    it "is exact with numbers too" do
      selector_proc = proc do |i|
        { :tag_number => rand(1e5) }
      end
      document_proc = proc do |i|
        { :lovability => rand }
      end
      assert_exact selector_proc, document_proc
    end
  end
end
