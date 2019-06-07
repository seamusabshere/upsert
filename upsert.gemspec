# -*- encoding: utf-8 -*-
require File.expand_path('../lib/upsert/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Seamus Abshere", "Phil Schalm"]
  gem.email         = ["seamus@abshere.net", "pnomolos@gmail.com"]
  t = %{Make it easy to upsert on MySQL, PostgreSQL, and SQLite3. Transparently creates merge functions for MySQL and PostgreSQL; on SQLite3, uses INSERT OR IGNORE.}
  gem.description   = t
  gem.summary       = t
  gem.homepage      = "https://github.com/seamusabshere/upsert"
  gem.license       = "MIT"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "upsert"
  gem.require_paths = ["lib"]
  gem.version       = Upsert::VERSION

  # NOTE: no runtime dependencies!

  gem.add_development_dependency 'rspec-core'
  gem.add_development_dependency 'rspec-expectations'
  gem.add_development_dependency 'rspec-mocks'

  gem.add_development_dependency 'activerecord', '~> 5'
  gem.add_development_dependency 'yard'
  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'pg-hstore', ">=1.1.3"
  gem.add_development_dependency 'sequel'
  gem.add_development_dependency 'rake', '~> 10'

  gem.add_development_dependency "activerecord-import", "~> 1" # 0.12 and up were failing

  gem.add_development_dependency "activerecord-postgresql-adapter"
  gem.add_development_dependency "sqlite3"
  gem.add_development_dependency "mysql2", "~> 0.5"
  gem.add_development_dependency "pg", "~> 1.1"
  # github-flavored markdown
  gem.add_development_dependency "redcarpet"
  gem.add_development_dependency "faker", "1.6.3"
end
