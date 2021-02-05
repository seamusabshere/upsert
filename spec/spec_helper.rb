# -*- encoding: utf-8 -*-
raise "A DB value is required" unless ENV["DB"]
ENV['DB'] = 'postgresql' if ENV['DB'].to_s =~ /postgresql/i
UNIQUE_CONSTRAINT = ENV['UNIQUE_CONSTRAINT'] == 'true'
raise "please use DB=postgresql NOT postgres" if ENV["DB"] == "postgres"

require 'bundler/setup'
Bundler.require(:default, :development)

require "active_record"
require "activerecord-import"
ActiveRecord::Base.default_timezone = :utc

require 'shellwords'
require "sequel"
Sequel.default_timezone = :utc
Sequel.extension :migration

class RawConnectionFactory
  DB_NAME = ENV['DB_NAME'] || 'upsert_test'
  # You *need* to specific DB_USER on certain combinations of JRuby/JDK as spawning a shell
  # has some oddities
  DB_USER = (ENV['DB_USER'] || `whoami`.chomp).to_s
  raise "A DB_USER value is required" if DB_USER.empty?
  DB_PASSWORD = ENV['DB_PASSWORD']
  DB_HOST = ENV['DB_HOST'] || '127.0.0.1'

  def self.db_env
    @db_env ||= base_params(nil, false).map { |k, v| [":#{k}", v.to_s.empty? ? nil : Shellwords.escape(v)] }.to_h
  end

  def self.adapter_name(adapter = nil)
    RUBY_PLATFORM != "java" && adapter == "mysql" ? "mysql2" : adapter
  end

  def self.base_params(adapter = nil, show_additional_params = true)
    return { :adapter => "sqlite3", :database => "temp.db", cache: "shared" } if adapter == "sqlite3"
    {
      host: DB_HOST,
      database: DB_NAME,
      dbname: DB_NAME,
      username: DB_USER,
      user: DB_USER,
      password: DB_PASSWORD,
      adapter: adapter,
    }.merge(
      show_additional_params ? additional_params(adapter) : {}
    )
  end

  def self.additional_params(adapter = nil)
    {
      "mysql" => { encoding: "utf8mb4" },
      "mysql2" => { encoding: "utf8mb4" },
    }.fetch(adapter, {})
  end

  def self.postgresql_call(string)
    Kernel.system "PGHOST=#{db_env[":host"]} PGUSER=#{db_env[":user"]} PGPASSWORD=#{db_env[":password"]} #{string.gsub(/:[a-z]+/, db_env)}"
  end

  def self.mysql_call(string)
    Kernel.system "mysql -h #{db_env[":host"]} -u #{db_env[":user"]} --password=#{db_env[":password"]} #{string.gsub(/:[a-z]+/, db_env)}"
  end

  SYSTEM_CALLS = {
    "postgresql" => [
      %{ dropdb :dbname },
      %{ createdb :dbname },
      %{ psql -d :dbname -c 'DROP SCHEMA IF EXISTS :dbname2 CASCADE' },
      %{ psql -d :dbname -c 'CREATE SCHEMA :dbname2' },
    ],
    "mysql" => [
      %{ -e "DROP DATABASE IF EXISTS :dbname" },
      %{ -e "DROP DATABASE IF EXISTS :dbname2" },
      %{ -e "CREATE DATABASE :dbname CHARSET utf8mb4 COLLATE utf8mb4_general_ci" },
      %{ -e "CREATE DATABASE :dbname2 CHARSET utf8mb4 COLLATE utf8mb4_general_ci" },
    ]
  }.freeze

  REQUIRES = {
    "mysql" => "mysql2",
    "postgresql" => "pg",
    "sqlite3" => "sqlite3",
    "java-postgresql" => "jdbc/postgres",
    "java-mysql" => "jdbc/mysql",
    "java-sqlite3" => "jdbc/sqlite3",
  }.freeze

  NEW_CONNECTION = {
    "postgresql" => ->(base_params) { PG::Connection.new(base_params.except(:database, :username, :adapter)) },
    "mysql" => ->(base_params) { Mysql2::Client.new(base_params) },
    "sqlite3" => ->(base_params) { ActiveRecord::Base.connection.raw_connection },
  }

  POST_CONNECTION = {
    "mysql" => -> { ActiveRecord::Base.connection.execute "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci" },
    "sqlite3" => -> { [ActiveRecord::Base.connection, ::DB].each { |c| c.execute "ATTACH DATABASE 'temp2.db' AS #{DB_NAME}2" } },
  }

  SYSTEM_CALLS.fetch(ENV["DB"], []).each do |str|
    send("#{ENV["DB"]}_call", str)
  end

  if RUBY_PLATFORM == 'java'
    CONFIG = "jdbc:#{ENV["DB"]}://#{DB_HOST}/#{DB_NAME}"
    require REQUIRES["java-#{ENV["DB"]}"]

    case ENV["DB"]
    when "postgresql" then Jdbc::Postgres.load_driver
    when "mysql"      then Jdbc::MySQL.load_driver
    when "sqlite3"
      Jdbc::SQLite3.load_driver
      CONFIG = "jdbc:sqlite::memory:?cache=shared"
    end

    def new_connection
      java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("+00:00"))
      java.sql.DriverManager.get_connection CONFIG, DB_USER, DB_PASSWORD
    end
  else
    case ENV['DB']
    when "postgresql", "mysql"
      require REQUIRES[ENV["DB"]]
      def new_connection
        NEW_CONNECTION[ENV["DB"]].call(self.class.base_params(ENV["DB"]))
      end
    when "sqlite3"
      require REQUIRES[ENV["DB"]]
      def new_connection
        NEW_CONNECTION[ENV["DB"]].call(self.class.base_params(ENV["DB"]))
      end
      CONFIG = { :adapter => "sqlite3", :database => "temp.db", cache: "shared" }

    end
  end

  ActiveRecord::Base.establish_connection(
    base_params(adapter_name(ENV["DB"]))
  )
  ari_adapter_name = adapter_name(ENV["DB"]) == "mysql" ? "mysql2" : adapter_name(ENV["DB"])
  require "activerecord-import/active_record/adapters/#{ari_adapter_name}_adapter"
end

raise "not supported" unless RawConnectionFactory.instance_methods.include?(:new_connection)

config = ActiveRecord::Base.connection.instance_variable_get(:@config)
config[:adapter] = case config[:adapter]
  when "postgresql" then "postgres"
  when "sqlite3" then "sqlite"
  else config[:adapter]
end
params = if RUBY_PLATFORM == "java"
  RawConnectionFactory::CONFIG
else
  config.merge(
    :user => config.values_at(:user, :username).compact.first,
    :host => config.values_at(:host, :hostaddr).compact.first,
    :database => config.values_at(:database, :dbname).compact.first
  )
end
DB = if RUBY_PLATFORM == "java"
  Sequel.connect(
    params,
    :user => RawConnectionFactory::DB_USER,
    :password => RawConnectionFactory::DB_PASSWORD,
    extensions: :activerecord_connection
  )
elsif ENV["DB"] == "sqlite3"
  Kernel.at_exit { FileUtils.rm(Dir.glob("temp*.db")) }
  Sequel.sqlite("temp.db", extensions: :activerecord_connection)
else
  Sequel.connect(params.merge(extensions: :activerecord_connection))
end

$conn_factory = RawConnectionFactory.new
$conn = $conn_factory.new_connection
RawConnectionFactory::POST_CONNECTION.fetch(ENV["DB"], -> {}).call


require 'logger'
require 'fileutils'
FileUtils.rm_f 'test.log'
ActiveRecord::Base.logger = Logger.new('test.log')

if ENV['VERBOSE'] == 'true'
  ActiveRecord::Base.logger.level = Logger::DEBUG
else
  ActiveRecord::Base.logger.level = Logger::WARN
end

if ENV['DB'] == 'postgresql'
  begin
    DB << "ALTER TABLE pets DROP CONSTRAINT IF EXISTS unique_name"
  rescue => e
    puts e.inspect
  end
end

class InternalMigration
  DEFINITIONS = {
    pets: ->(db) {
      primary_key :id
      String :name, { size: 191 }.merge(ENV["DB"] == "mysql" || UNIQUE_CONSTRAINT ? { index: { unique: true } } : {})
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
    },
    tasks: ->(db) {
      primary_key :id
      String :name
      DateTime :created_at
      DateTime :created_on
    },
    people: ->(db) {
      primary_key :id
      String :"First Name"
      String :"Last Name"
    },
    alphabets: ->(db) {
      ("a".."z").each do |col|
        Integer "the_letter_#{col}".to_sym
      end
    }
  }
end

Sequel.migration do
  change do
    db = self
    InternalMigration::DEFINITIONS.each do |table, blk|
      create_table!(table) do
        instance_exec(db, &blk)
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

  def clone_ar_class(klass, table_name)
    u = Upsert.new $conn, klass.table_name
    new_table_name = [*table_name].compact
    # AR's support for quoting of schema and table names is horrendous
    # schema.table and schema.`table` are considiered different names on MySQL, but
    # schema.table and schema."table" are correctly considered the same on Postgres
    sequel_table_name = new_table_name.map(&:to_sym)
    new_table_name[-1] = u.connection.quote_ident(new_table_name[-1]) if new_table_name[-1].to_s.index('.')
    new_table_name = new_table_name.join('.')

    Sequel.migration do
      change do
        db = self
        create_table!(sequel_table_name.length > 1 ? Sequel.qualify(*sequel_table_name) : sequel_table_name.first) do
          instance_exec(db, &InternalMigration::DEFINITIONS[klass.table_name.to_sym])
        end
      end
    end.apply(DB, :up)

    cls = Class.new(klass)
    cls.class_eval do
      self.table_name = new_table_name
      def self.quoted_table_name
        new_table_name
      end
    end
    cls
  end
end

RSpec.configure do |c|
  c.include SpecHelper

  c.filter_run_when_matching :focus
  c.order = :defined
  c.warnings = true
  c.full_backtrace = false

  c.before do
    Pet.delete_all
  end
end

require 'upsert'
