require 'rubygems'
require 'bundler/setup'
require 'minitest/spec'
require 'minitest/autorun'
require 'minitest/reporters'
MiniTest::Unit.runner = MiniTest::SuiteRunner.new
MiniTest::Unit.runner.reporters << MiniTest::Reporters::SpecReporter.new

require 'active_record'
require 'active_record_inline_schema'

require 'logger'
ActiveRecord::Base.logger = Logger.new($stdout)
ActiveRecord::Base.logger.level = Logger::DEBUG

class Pet < ActiveRecord::Base
  self.primary_key = 'name'
  col :name
  col :gender
end

require 'upsert'

MiniTest::Spec.class_eval do
  def self.shared_examples
    @shared_examples ||= {}
  end

  def assert_creates(model, expected_records)
    expected_records.each do |conditions|
      model.count(:conditions => conditions).must_equal 0
    end
    yield
    expected_records.each do |conditions|
      model.count(:conditions => conditions).must_equal 1
    end
  end

end

module MiniTest::Spec::SharedExamples
  def shared_examples_for(desc, &block)
    MiniTest::Spec.shared_examples[desc] = block
  end

  def it_behaves_like(desc)
    self.instance_eval do
      MiniTest::Spec.shared_examples[desc].call
    end
  end
end

Object.class_eval { include(MiniTest::Spec::SharedExamples) }
require 'shared_examples'
