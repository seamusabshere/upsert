#!/usr/bin/env rake
require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = "--format documentation"
end

task :default => :spec

task :rspec_all_databases do
  results = {}
  
  dbs = %w{ postgresql mysql sqlite3 }
  if ENV['DB']
    dbs = ENV['DB'].split(',') 
  end
  
  dbs.each do |db|
    puts
    puts '#'*50
    puts "# Running specs against #{db}"
    puts '#'*50
    puts

    if RUBY_VERSION >= '1.9'
      pid = spawn({'DB' => db}, 'rspec', '--format', 'documentation', File.expand_path('../spec', __FILE__))
      Process.waitpid pid
      results[db] = $?.success?
    else
      exec({'DB' => db}, 'rspec', '--format', 'documentation', File.expand_path('../spec', __FILE__))
    end

  end
  puts results.inspect
end

task :n, :from, :to do |t, args|
  Dir[File.expand_path("../lib/upsert/**/#{args.from}.*", __FILE__)].each do |path|
    dir = File.dirname(path)
    File.open("#{dir}/#{args.to}.rb", 'w') do |f|
      f.write File.read(path).gsub(args.from, args.to)
    end
  end
end

require 'yard'
YARD::Rake::YardocTask.new
