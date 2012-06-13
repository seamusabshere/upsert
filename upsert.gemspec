# -*- encoding: utf-8 -*-
require File.expand_path('../lib/upsert/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Seamus Abshere"]
  gem.email         = ["seamus@abshere.net"]
  gem.description   = %q{TODO: Write a gem description}
  gem.summary       = %q{TODO: Write a gem summary}
  gem.homepage      = ""

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "upsert"
  gem.require_paths = ["lib"]
  gem.version       = Upsert::VERSION

  gem.add_development_dependency 'sqlite3'
  gem.add_development_dependency 'mysql2'
  gem.add_development_dependency 'pg'
  gem.add_development_dependency 'minitest'
  gem.add_development_dependency 'minitest-reporters'
  gem.add_development_dependency 'yard'
end
