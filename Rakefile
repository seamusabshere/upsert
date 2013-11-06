#!/usr/bin/env rake
require "bundler/gem_tasks"

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
    # won't work on 1.8.7...
    pid = Kernel.spawn({'DB' => db}, 'rspec', '--format', 'documentation', File.expand_path('../spec', __FILE__))
    Process.waitpid pid
    results[db] = $?.success? 
  end
  puts results.inspect
end

task :default => :rspec_all_databases

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
