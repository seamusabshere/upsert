require 'rubygems'
require 'bundler/setup'
require 'minitest/spec'
require 'minitest/autorun'
require 'minitest/reporters'
MiniTest::Unit.runner = MiniTest::SuiteRunner.new
MiniTest::Unit.runner.reporters << MiniTest::Reporters::SpecReporter.new

require 'upsert'

MiniTest::Spec.class_eval do
  def self.shared_examples
    @shared_examples ||= {}
  end

  def assert_count(expected_count, table_name, row)
    sql = count_sql table_name, row
    actual_count = select_one(sql).to_i
    actual_count.must_equal expected_count
  end

  def assert_creates(table_name, rows)
    rows.each do |row|
      assert_count 0, table_name, row
    end
    yield
    rows.each do |row|
      assert_count 1, table_name, row
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
