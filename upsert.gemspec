# -*- encoding: utf-8 -*-
require File.expand_path('../lib/upsert/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Seamus Abshere", "Phil Schalm"]
  gem.email         = ["seamus@abshere.net"]
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

  gem.add_development_dependency 'activerecord', '~>3'
  gem.add_development_dependency 'active_record_inline_schema'
  gem.add_development_dependency 'yard'
  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'pg-hstore', ">=1.1.3"
  gem.add_development_dependency 'sequel'
  gem.add_development_dependency 'rake', '~>10.1.1'

  if RUBY_VERSION >= '1.9'
    gem.add_development_dependency 'activerecord-import', '0.11.0' # 0.12 and up were failing
  else
    gem.add_development_dependency 'orderedhash'
  end

  if RUBY_PLATFORM == 'java'
    gem.add_development_dependency 'jruby-openssl'
    gem.add_development_dependency 'jdbc-postgres'
    gem.add_development_dependency 'jdbc-mysql'
    gem.add_development_dependency 'jdbc-sqlite3'
    gem.add_development_dependency 'activerecord-jdbcsqlite3-adapter'
    gem.add_development_dependency 'activerecord-jdbcmysql-adapter'
    gem.add_development_dependency 'activerecord-jdbcpostgresql-adapter'
  else
    gem.add_development_dependency 'activerecord-postgresql-adapter'
    gem.add_development_dependency 'sqlite3'
    gem.add_development_dependency 'mysql2', '~> 0.3.10'
    gem.add_development_dependency 'pg', '~> 0.18.0'
    # github-flavored markdown
    if RUBY_VERSION >= '1.9'
      gem.add_development_dependency 'redcarpet'
    else
      gem.add_development_dependency 'redcarpet', '~> 2.3.0'
    end
  end

  if RUBY_VERSION <= '1.9.3'
    gem.add_development_dependency 'faker', '1.6.3'
  else
    gem.add_development_dependency 'faker'
  end
end
