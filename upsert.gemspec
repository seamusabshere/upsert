# -*- encoding: utf-8 -*-
require File.expand_path('../lib/upsert/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Seamus Abshere"]
  gem.email         = ["seamus@abshere.net"]
  t = %{Ruby library to make it easy to upsert on MySQL, PostgreSQL, and SQLite3. Uses MySQL's ON DUPLICATE KEY UPDATE, PostgreSQL's CREATE FUNCTION merge_db, and SQLite's INSERT OR IGNORE.}
  gem.description   = t
  gem.summary       = t
  gem.homepage      = "https://github.com/seamusabshere/upsert"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "upsert"
  gem.require_paths = ["lib"]
  gem.version       = Upsert::VERSION

  gem.add_development_dependency 'posix-spawn'
  gem.add_development_dependency 'rspec-core'
  gem.add_development_dependency 'rspec-expectations'
  gem.add_development_dependency 'rspec-mocks'

  gem.add_development_dependency 'sqlite3'
  gem.add_development_dependency 'mysql2'
  gem.add_development_dependency 'pg'
  gem.add_development_dependency 'activerecord' # testing only
  gem.add_development_dependency 'active_record_inline_schema'
  gem.add_development_dependency 'faker'
  gem.add_development_dependency 'yard'
  gem.add_development_dependency 'redcarpet' # github-flavored markdown
  gem.add_development_dependency 'activerecord-import'
  gem.add_development_dependency 'pry'

  unless RUBY_VERSION >= '1.9'
    gem.add_development_dependency 'orderedhash'
  end
end
