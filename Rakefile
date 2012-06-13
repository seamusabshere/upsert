#!/usr/bin/env rake
require "bundler/gem_tasks"

require 'rake'
require 'rake/testtask'
Rake::TestTask.new(:_test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

task :test_each_db_adapter do
  %w{ mysql2 sqlite pg }.each do |database|
    puts
    puts "#{'*'*10} Running #{database} tests"
    puts
    puts `rake _test TEST=test/test_#{database}.rb`
  end
end

task :default => :test_each_db_adapter
task :test => :test_each_db_adapter

require 'yard'
YARD::Rake::YardocTask.new
