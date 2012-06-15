require 'rubygems'
require 'bundler/setup'
require 'faker'
require 'benchmark'
require 'minitest/spec'
require 'minitest/autorun'
require 'minitest/reporters'
MiniTest::Unit.runner = MiniTest::SuiteRunner.new
MiniTest::Unit.runner.reporters << MiniTest::Reporters::SpecReporter.new

require 'active_record'
require 'active_record_inline_schema'

# require 'logger'
# ActiveRecord::Base.logger = Logger.new($stdout)
# ActiveRecord::Base.logger.level = Logger::DEBUG

class Pet < ActiveRecord::Base
  self.primary_key = 'name'
  col :name
  col :gender
  col :morning_walk_time, :type => :datetime
  col :zipped_biography, :type => :binary
  col :tag_number, :type => :integer
  col :birthday, :type => :datetime
  col :home_address, :type => :text
end

require 'upsert'

MiniTest::Spec.class_eval do
  def self.shared_examples
    @shared_examples ||= {}
  end

  def assert_creates(model, expected_records)
    expected_records.each do |conditions|
      model.where(conditions).count.must_equal 0
    end
    yield
    expected_records.each do |conditions|
      model.where(conditions).count.must_equal 1
    end
  end

  def assert_faster_than(competition, records, &blk)
    # dry run
    blk.call records
    ref1 = Pet.order(:name).all.map(&:attributes)
    Pet.delete_all
    # --
    
    ar_time = Benchmark.realtime { blk.call(records) }
    ref2 = Pet.order(:name).all.map(&:attributes)
    ref2.must_equal ref1
    Pet.delete_all

    upsert_time = Benchmark.realtime do
      upsert = Upsert.new connection, :pets
      upsert.multi do |xxx|
        records.each do |selector, document|
          xxx.row(selector, document)
        end
      end
    end
    ref3 = Pet.order(:name).all.map(&:attributes)
    ref3.must_equal ref1
    upsert_time.must_be :<, ar_time
    $stderr.puts "   Upsert was #{((ar_time - upsert_time) / ar_time * 100).round}% faster than #{competition}"
  end
end

module MiniTest::Spec::SharedExamples
  def shared_examples_for(desc, &block)
    MiniTest::Spec.shared_examples[desc] = block
  end

  def it_also(desc)
    self.instance_eval do
      MiniTest::Spec.shared_examples[desc].call
    end
  end
end

Object.class_eval { include(MiniTest::Spec::SharedExamples) }
Dir[File.expand_path("../shared/*.rb", __FILE__)].each do |path|
  require path
end
