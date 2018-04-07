# -*- encoding: utf-8 -*-
require 'bundler/setup'
Bundler.require(:default, :development)

# require 'pry'
require 'shellwords'

require 'active_record'
ActiveRecord::Base.default_timezone = :utc

module ActiveRecordInlineSchema::ActiveRecordClassMethods
  def reset_model!
    inline_schema_config.instance_variable_set(:@model, self)
    inline_schema_config.send(:safe_reset_column_information)
    inline_schema_config.instance_variable_set(:@ideal_indexes, ::Set.new(
      inline_schema_config.ideal_indexes.map do |idx|
        ActiveRecordInlineSchema::Config::Index.new(
          self.inline_schema_config,
          idx.column_name,
          idx.initial_options
        )
      end
    ))
  end

  def inline_schema_config
    return @inline_schema_config if defined?(@inline_schema_config)
    if superclass != ::ActiveRecord::Base
      @inline_schema_config = base_class.inline_schema_config.dup
    else
      MUTEX.synchronize do
        @inline_schema_config = ::ActiveRecordInlineSchema::Config.new self
      end
    end
  end
end

require 'active_record_inline_schema'

require 'activerecord-import' if RUBY_VERSION >= '1.9'

ENV['DB'] ||= 'mysql'
ENV['DB'] = 'postgresql' if ENV['DB'].to_s =~ /postgresql/
UNIQUE_CONSTRAINT = ENV['UNIQUE_CONSTRAINT'] == 'true'


class RawConnectionFactory
  DATABASE = 'upsert_test'
  CURRENT_USER = (ENV['DB_USER'] || `whoami`.chomp)
  PASSWORD = ENV['DB_PASSWORD']
  HOST = ENV['DB_HOST'] || '127.0.0.1'

  case ENV['DB']

  when 'postgresql'
    Kernel.system %{ dropdb upsert_test }
    Kernel.system %{ createdb upsert_test }
    Kernel.system %{ psql -d upsert_test -c 'CREATE SCHEMA #{DATABASE}2' }
    if RUBY_PLATFORM == 'java'
      CONFIG = "jdbc:postgresql://localhost/#{DATABASE}?user=#{CURRENT_USER}"
      require 'jdbc/postgres'
      # http://thesymanual.wordpress.com/2011/02/21/connecting-jruby-to-postgresql-with-jdbc-postgre-api/
      Jdbc::Postgres.load_driver
      # java.sql.DriverManager.register_driver org.postgresql.Driver.new
      Java::JavaClass.for_name("org.postgresql.Driver")
      def new_connection
        java.sql.DriverManager.get_connection CONFIG
      end
    else
      CONFIG = { :dbname => DATABASE }
      require 'pg'
      def new_connection
        PG::Connection.new CONFIG
      end
    end
    ActiveRecord::Base.establish_connection :adapter => 'postgresql', :database => DATABASE, :username => CURRENT_USER

  when 'mysql'
    password_argument = (PASSWORD.nil?) ? "" : "--password=#{Shellwords.escape(PASSWORD)}"
    Kernel.system %{ mysql -h #{HOST} -u #{CURRENT_USER} #{password_argument} -e "DROP DATABASE IF EXISTS #{DATABASE}" }
    Kernel.system %{ mysql -h #{HOST} -u #{CURRENT_USER} #{password_argument} -e "DROP DATABASE IF EXISTS #{DATABASE}2" }
    Kernel.system %{ mysql -h #{HOST} -u #{CURRENT_USER} #{password_argument} -e "CREATE DATABASE #{DATABASE} CHARSET utf8mb4 COLLATE utf8mb4_general_ci" }
    Kernel.system %{ mysql -h #{HOST} -u #{CURRENT_USER} #{password_argument} -e "CREATE DATABASE #{DATABASE}2 CHARSET utf8mb4 COLLATE utf8mb4_general_ci" }
    if RUBY_PLATFORM == 'java'
      CONFIG = "jdbc:mysql://#{HOST}/#{DATABASE}?user=#{CURRENT_USER}&password=#{PASSWORD}"
      require 'jdbc/mysql'
      Jdbc::MySQL.load_driver
      # java.sql.DriverManager.register_driver com.mysql.jdbc.Driver.new
      def new_connection
        java.sql.DriverManager.get_connection CONFIG
      end
    else
      require 'mysql2'
      def new_connection
        config = { :username => CURRENT_USER, :database => DATABASE, :host => HOST, :encoding => 'utf8mb4' }
        config.merge!(:password => PASSWORD) unless PASSWORD.nil?
        Mysql2::Client.new config
      end
    end
    ActiveRecord::Base.establish_connection(
      :adapter => RUBY_PLATFORM == 'java' ? 'mysql' : 'mysql2',
      :user => CURRENT_USER,
      :password => PASSWORD,
      :host => HOST,
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
    ActiveRecord::Base.connection.execute "ATTACH DATABASE ':memory:' AS #{DATABASE}2"

  when 'postgres'
    raise "please use DB=postgresql NOT postgres"

  else
    raise "not supported"
  end
end

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
    const.connection.execute("ALTER TABLE pets DROP CONSTRAINT IF EXISTS unique_name")
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

  def clone_ar_class(klass, table_name)
    u = Upsert.new $conn, klass.table_name
    new_table_name = [*table_name].compact
    # AR's support for quoting of schema and table names is horrendous
    # schema.table and schema.`table` are considiered different names on MySQL, but
    # schema.table and schema."table" are correctly considered the same on Postgres
    new_table_name[-1] = u.connection.quote_ident(new_table_name[-1]) if new_table_name[-1].to_s.index('.')
    new_table_name = new_table_name.join('.')
    cls = Class.new(klass)
    cls.class_eval do
      self.table_name = new_table_name
      def self.quoted_table_name
        table_name
      end

      reset_model!
    end
    cls.auto_upgrade!
    cls
  end
end

RSpec.configure do |c|
  c.include SpecHelper
  c.before do
    Pet.delete_all
  end
end

require 'upsert'
