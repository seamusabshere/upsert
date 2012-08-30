#!/usr/bin/env rake
require "bundler/gem_tasks"

task :rspec_all_databases do
  require 'posix-spawn'
  %w{ postgresql mysql2 sqlite3 }.each do |adapter|
    puts
    puts '#'*50
    puts "# Running specs against #{adapter}"
    puts '#'*50
    puts
    pid = POSIX::Spawn.spawn({'ADAPTER' => adapter}, 'rspec', '--format', 'documentation', File.expand_path('../spec', __FILE__))
    Process.waitpid pid
  end
end

task :default => :rspec_all_databases

require 'yard'
YARD::Rake::YardocTask.new
