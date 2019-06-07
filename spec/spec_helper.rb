# -*- encoding: utf-8 -*-
require 'bundler/setup'

# require 'pry'
require 'shellwords'

require "sequel"
Sequel.default_timezone = :utc
Sequel.extension :migration

require "active_record"
require "activerecord-import"
ActiveRecord::Base.default_timezone = :utc

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
    require "activerecord-import/active_record/adapters/postgresql_adapter"

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
      :user => CURRENT_USER,
      :password => PASSWORD,
      :host => DB_HOST,
      :database => DATABASE,
      :encoding => 'utf8mb4'
    )
    ActiveRecord::Base.connection.execute "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"
    require "activerecord-import/active_record/adapters/mysql2_adapter"

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
    require "activerecord-import/active_record/adapters/sqlite3_adapter"

  when 'postgres'
    raise "please use DB=postgresql NOT postgres"

  else
    raise "not supported"
  end
end

config = ActiveRecord::Base.connection.instance_variable_get(:@config)
config[:adapter] = case config[:adapter]
  when "postgresql" then "postgres"
  else config[:adapter]
end
params = if RUBY_PLATFORM == "java"
  RawConnectionFactory::CONFIG
else
  config.merge(
    :user => config.values_at(:user, :username).compact.first,
    :host => config.values_at(:host, :hostaddr).compact.first
  )
end
DB = Sequel.connect(params)

$conn_factory = RawConnectionFactory.new
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

if ENV['DB'] == 'postgresql' && UNIQUE_CONSTRAINT
  begin
    DB << "ALTER TABLE pets DROP CONSTRAINT IF EXISTS unique_name"
  rescue => e
    puts e.inspect
  end
end

Sequel.migration do
  change do
    db = self
    create_table?(:pets) do
      primary_key :id
      String :name, size: 191, index: { unique: true }
      String :gender
      String :spiel
      TrueClass :good
      BigDecimal :lovability, size: [30, 15] # 15 integer digits and 15 fractional digits
      DateTime :morning_walk_time
      File :zipped_biography
      Integer :tag_number
      Bignum :big_tag_number
      Date :birthday
      String :home_address, text: true

      if db.database_type == :postgres
        column :tsntz, "timestamp without time zone"
      end
    end

    create_table?(:tasks) do
      primary_key :id
      String :name
      DateTime :created_at
      DateTime :created_on
    end

    create_table?(:people) do
      primary_key :id
      String :"First Name"
      String :"Last Name"
    end

    create_table?(:alphabets) do
      ("a".."z").each do |col|
        Integer "the_letter_#{col}".to_sym
      end
    end
  end
end.apply(DB, :up)

if ENV['DB'] == 'postgresql' && UNIQUE_CONSTRAINT
  DB << "ALTER TABLE pets ADD CONSTRAINT unique_name UNIQUE (name)"
end

%i[Pet Task Person Alphabet].each do |name|
  Object.const_set(name, Class.new(ActiveRecord::Base))
end

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
          :lovability => BigDecimal(rand(1e11).to_s, 2),
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
