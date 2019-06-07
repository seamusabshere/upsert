# -*- encoding: utf-8 -*-
require 'bundler/setup'

# require 'pry'
require 'shellwords'

require 'active_record'
ActiveRecord::Base.default_timezone = :utc
require 'activerecord-jdbc-adapter' if defined? JRUBY_VERSION

require 'active_record_inline_schema'

require 'activerecord-import' if RUBY_VERSION >= '1.9'

ENV['DB'] ||= 'mysql'
ENV['DB'] = 'postgresql' if ENV['DB'].to_s =~ /postgresql/
UNIQUE_CONSTRAINT = ENV['UNIQUE_CONSTRAINT'] == 'true'

class RawConnectionFactory
  DATABASE = 'upsert_test'
  CURRENT_USER = (ENV['DB_USER'] || `whoami`.chomp)
  PASSWORD = ENV['DB_PASSWORD']
  DB_HOST = ENV['DB_HOST'] || '127.0.0.1'

  case ENV['DB']
  when 'postgresql'
    Kernel.system %{ PGHOST=#{DB_HOST} PGUSER=#{CURRENT_USER} PGPASSWORD=#{PASSWORD} dropdb #{DATABASE} }
    Kernel.system %{ PGHOST=#{DB_HOST} PGUSER=#{CURRENT_USER} PGPASSWORD=#{PASSWORD} createdb #{DATABASE} }
    if RUBY_PLATFORM == 'java'
      CONFIG = "jdbc:postgresql://#{DB_HOST}/#{DATABASE}"
      require 'jdbc/postgres'
      # http://thesymanual.wordpress.com/2011/02/21/connecting-jruby-to-postgresql-with-jdbc-postgre-api/
      Jdbc::Postgres.load_driver
      # java.sql.DriverManager.register_driver org.postgresql.Driver.new
      def new_connection
        java.sql.DriverManager.get_connection CONFIG, CURRENT_USER, PASSWORD
      end
    else
      CONFIG = { :dbname => DATABASE, :host => DB_HOST, :user => CURRENT_USER }
      CONFIG.merge!(:password => PASSWORD) unless PASSWORD.nil?
      require 'pg'
      def new_connection
        PG::Connection.new CONFIG
      end
    end
    ActiveRecord::Base.establish_connection(
      :hostaddr => DB_HOST, 
      :adapter => 'postgresql', 
      :dbname => DATABASE, 
      :username => CURRENT_USER, 
      :password => PASSWORD
    )

  when 'mysql'
    password_argument = (PASSWORD.nil?) ? "" : "--password=#{Shellwords.escape(PASSWORD)}"
    Kernel.system %{ mysql -h #{DB_HOST} -u #{CURRENT_USER} #{password_argument} -e "DROP DATABASE IF EXISTS #{DATABASE}" }
    Kernel.system %{ mysql -h #{DB_HOST} -u #{CURRENT_USER} #{password_argument} -e "CREATE DATABASE #{DATABASE} CHARSET utf8mb4 COLLATE utf8mb4_general_ci" }
    if RUBY_PLATFORM == 'java'
      CONFIG = "jdbc:mysql://#{DB_HOST}/#{DATABASE}"
      require 'jdbc/mysql'
      Jdbc::MySQL.load_driver
      # java.sql.DriverManager.register_driver com.mysql.jdbc.Driver.new
      def new_connection
        java.sql.DriverManager.get_connection CONFIG, CURRENT_USER, PASSWORD
      end
    else
      require 'mysql2'
      def new_connection
        config = { :username => CURRENT_USER, :database => DATABASE, :host => DB_HOST, :encoding => 'utf8mb4' }
        config.merge!(:password => PASSWORD) unless PASSWORD.nil?
        Mysql2::Client.new config
      end
    end
    ActiveRecord::Base.establish_connection(
      :adapter => RUBY_PLATFORM == 'java' ? 'mysql' : 'mysql2',
      :username => CURRENT_USER,
      :password => PASSWORD,
      :host => DB_HOST,
      :database => DATABASE,
      :encoding => 'utf8mb4'
    )
    ActiveRecord::Base.connection.execute "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"

  when 'sqlite3'
    CONFIG = { :adapter => 'sqlite3', :database => 'file::memory:?cache=shared' }
    if RUBY_PLATFORM == 'java'
      # CONFIG = 'jdbc:sqlite://test.sqlite3'
      require 'jdbc/sqlite3'
      Jdbc::SQLite3.load_driver
      def new_connection
        ActiveRecord::Base.connection.raw_connection.connection
      end
    else
      require 'sqlite3'
      def new_connection
        ActiveRecord::Base.connection.raw_connection
      end
    end
    ActiveRecord::Base.establish_connection CONFIG

  when 'postgres'
    raise "please use DB=postgresql NOT postgres"

  else
    raise "not supported"
  end
end

$conn_factory = RawConnectionFactory.new

# Fix a regression in activerecord-jdbc-adapter v1.3.25
if ArJdbc::VERSION == "1.3.25"
  module ActiveRecord
    module ConnectionAdapters
      class JdbcDriver
        def connection(url, user, pass)
          # bypass DriverManager to get around problem with dynamically loaded jdbc drivers
          properties = self.properties.dup
          properties.setProperty("user", user.to_s) if user
          properties.setProperty("password", pass.to_s) if pass
          @driver.connect(url, properties)
        end
      end
    end
  end
end

$conn = $conn_factory.new_connection

require 'logger'
require 'fileutils'
FileUtils.rm_f 'test.log'
ActiveRecord::Base.logger = Logger.new('test.log')

if ENV['VERBOSE'] == 'true'
  ActiveRecord::Base.logger.level = Logger::DEBUG
else
  ActiveRecord::Base.logger.level = Logger::WARN
end

class Pet < ActiveRecord::Base
  col :name, limit: 191 # utf8mb4 in mysql requirement
  col :gender
  col :spiel
  col :good, :type => :boolean
  col :lovability, :type => :float
  col :morning_walk_time, :type => :datetime
  col :zipped_biography, :type => :binary
  col :tag_number, :type => :integer
  col :big_tag_number, :type => :bigint
  col :birthday, :type => :date
  col :home_address, :type => :text
  if ENV['DB'] == 'postgresql'
    col :tsntz, :type => 'timestamp without time zone'
  end
  add_index :name, :unique => true
end
if ENV['DB'] == 'postgresql' && UNIQUE_CONSTRAINT
  begin
    Pet.connection.execute("ALTER TABLE pets DROP CONSTRAINT IF EXISTS unique_name")
  rescue => e
    puts e.inspect
  end
end

Pet.auto_upgrade!

if ENV['DB'] == 'postgresql' && UNIQUE_CONSTRAINT
  Pet.connection.execute("ALTER TABLE pets ADD CONSTRAINT unique_name UNIQUE (name)")
end

class Task < ActiveRecord::Base
  col :name
  col :created_at, :type => :datetime
  col :created_on, :type => :datetime
end
Task.auto_upgrade!

class Person < ActiveRecord::Base
  col :"First Name"
  col :"Last Name"
end
Person.auto_upgrade!

class Alphabet < ActiveRecord::Base
  ('a'..'z').each do |col|
    col "the_letter_#{col}".to_sym, :type => :integer
  end
end
Alphabet.auto_upgrade!

require 'zlib'
require 'benchmark'
require 'faker'

module SpecHelper
  def random_time_or_datetime
    time = Time.at(rand * Time.now.to_i)
    if ENV['DB'] == 'mysql'
      time = time.change(:usec => 0)
    end
    if rand > 0.5
      time = time.change(:usec => 0).to_datetime
    end
    time
  end

  def lotsa_records
    @records ||= begin
      memo = []
      names = []
      333.times do
        names << Faker::Name.name
      end
      2000.times do
        selector = ActiveSupport::OrderedHash.new
        selector[:name] = if RUBY_VERSION >= '1.9'
          names.sample
        else
          names.choice
        end
        setter = {
          :lovability => BigDecimal.new(rand(1e11).to_s, 2),
          :tag_number => rand(1e8),
          :spiel => Faker::Lorem.sentences.join,
          :good => true,
          :birthday => Time.at(rand * Time.now.to_i).to_date,
          :morning_walk_time => random_time_or_datetime,
          :home_address => Faker::Lorem.sentences.join,
          # hard to know how to have AR insert this properly unless Upsert::Binary subclasses String
          # :zipped_biography => Upsert.binary(Zlib::Deflate.deflate(Faker::Lorem.paragraphs.join, Zlib::BEST_SPEED))
        }
        memo << [selector, setter]
      end
      memo
    end
  end

  def assert_same_result(records, &blk)
    blk.call(records)
    ref1 = Pet.order(:name).all.map { |pet| pet.attributes.except('id') }

    Pet.delete_all

    Upsert.batch($conn, :pets) do |upsert|
      records.each do |selector, setter|
        upsert.row(selector, setter)
      end
    end
    ref2 = Pet.order(:name).all.map { |pet| pet.attributes.except('id') }
    compare_attribute_sets ref1, ref2
  end

  def assert_creates(model, expected_records)
    expected_records.each do |selector, setter|
      # should i use setter in where?
      model.where(selector).count.should == 0
    end
    yield
    expected_records.each do |selector, setter|
      setter ||= {}
      found = model.where(selector).map { |record| record.attributes.except('id') }
      expect(found).to_not be_empty, { :selector => selector, :setter => setter }.inspect
      expected = [ selector.stringify_keys.merge(setter.stringify_keys) ]
      compare_attribute_sets expected, found
    end
  end

  def compare_attribute_sets(expected, found)
    e = expected.map { |attrs| simplify_attributes attrs }
    f = found.map { |attrs| simplify_attributes attrs }
    f.each_with_index do |fa, i|
      fa.should == e[i]
    end
  end

  def simplify_attributes(attrs)
    attrs.select do |k, v|
      v.present?
    end.inject({}) do |memo, (k, v)|
      memo[k] = case v
      when Time, DateTime
        v.to_i
      else
        v
      end
      memo
    end
  end

  def assert_faster_than(competition, records, &blk)
    # dry run
    blk.call records
    Pet.delete_all
    sleep 1
    # --

    ar_time = Benchmark.realtime { blk.call(records) }

    Pet.delete_all
    sleep 1

    upsert_time = Benchmark.realtime do
      Upsert.batch($conn, :pets) do |upsert|
        records.each do |selector, setter|
          upsert.row(selector, setter)
        end
      end
    end
    upsert_time.should be < ar_time
    $stderr.puts "   Upsert was #{((ar_time - upsert_time) / ar_time * 100).round}% faster than #{competition}"
  end
end

RSpec.configure do |c|
  c.include SpecHelper
  c.before do
    Pet.delete_all
  end
end

require 'upsert'
