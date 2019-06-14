Gem::Specification.load(File.expand_path("upsert.gemspec.common", __dir__)).tap do |gem|
  gem.instance_exec do
    add_development_dependency "activerecord-postgresql-adapter"
    add_development_dependency "sqlite3"
    add_development_dependency "mysql2", "~> 0.5"
    add_development_dependency "pg", "~> 1.1"
  end
end