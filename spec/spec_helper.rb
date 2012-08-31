require 'bundler/setup'

require 'active_record'
require 'active_record_inline_schema'
require 'activerecord-import'

ENV['ADAPTER'] ||= 'mysql2'

case ENV['ADAPTER']
when 'postgresql'
  system %{ dropdb upsert_test }
  system %{ createdb upsert_test }
  ActiveRecord::Base.establish_connection :adapter => 'postgresql', :database => 'upsert_test'
  $conn = PGconn.new(:dbname => 'upsert_test')
when 'mysql2'
  system %{ mysql -u root -ppassword -e "DROP DATABASE IF EXISTS upsert_test" }
  system %{ mysql -u root -ppassword -e "CREATE DATABASE upsert_test CHARSET utf8" }
  ActiveRecord::Base.establish_connection 'mysql2://root:password@127.0.0.1/upsert_test'
  $conn = Mysql2::Client.new(:username => 'root', :password => 'password', :database => 'upsert_test')
when 'sqlite3'
  ActiveRecord::Base.establish_connection :adapter => 'sqlite3', :database => ':memory:'
  $conn = ActiveRecord::Base.connection.raw_connection
else
  raise "not supported"
end

if ENV['UPSERT_DEBUG'] == 'true'
  require 'logger'
  ActiveRecord::Base.logger = Logger.new($stdout)
  ActiveRecord::Base.logger.level = Logger::DEBUG
end

class Pet < ActiveRecord::Base
  col :name
  col :gender
  col :spiel
  col :good, :type => :boolean
  col :lovability, :type => :float
  col :morning_walk_time, :type => :datetime
  col :zipped_biography, :type => :binary
  col :tag_number, :type => :integer
  col :birthday, :type => :date
  col :home_address, :type => :text
  add_index :name, :unique => true
end
Pet.auto_upgrade!

require 'securerandom'
require 'zlib'
require 'benchmark'
require 'faker'

module SpecHelper
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
          names.sample(1).first
        else
          names.choice
        end
        document = {
          :lovability => BigDecimal.new(rand(1e11).to_s, 2),
          :tag_number => rand(1e8),
          :spiel => SecureRandom.hex(rand(127)),
          :good => true,
          :birthday => Time.at(rand * Time.now.to_i).to_date,
          :morning_walk_time => Time.at(rand * Time.now.to_i),
          :home_address => SecureRandom.hex(rand(1000)),
          :zipped_biography => Upsert.binary(Zlib::Deflate.deflate(SecureRandom.hex(rand(1000)), Zlib::BEST_SPEED))
        }
        memo << [selector, document]
      end
      memo
    end
  end

  def assert_same_result(records, &blk)
    blk.call(records)
    ref1 = Pet.order(:name).all.map { |pet| pet.attributes.except('id') }
    
    Pet.delete_all

    Upsert.batch($conn, :pets) do |upsert|
      records.each do |selector, document|
        upsert.row(selector, document)
      end
    end
    ref2 = Pet.order(:name).all.map { |pet| pet.attributes.except('id') }
    ref2.should == ref1
  end

  def assert_creates(model, expected_records)
    expected_records.each do |conditions|
      model.where(conditions).count.should == 0
    end
    yield
    expected_records.each do |conditions|
      model.where(conditions).count.should == 1
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
        records.each do |selector, document|
          upsert.row(selector, document)
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
