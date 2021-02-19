Gem::Specification.load(File.expand_path("upsert.gemspec.common", __dir__)).dup.tap do |gem|
  gem.instance_exec do
    self.name = "upsert"
    self.platform = "java"

    add_development_dependency "jruby-openssl"
    add_development_dependency "jdbc-postgres"
    add_development_dependency "jdbc-mysql", "< 8"
    add_development_dependency "jdbc-sqlite3"
    add_development_dependency "activerecord-jdbc-adapter"

    add_development_dependency "pry-nav" unless ENV["CI"]
  end
end
