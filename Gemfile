source 'https://rubygems.org'

# Specify your gem's dependencies in upsert.gemspec

if Gem::Version.new(Bundler::VERSION) >= Gem::Version.new("2.0.0")
  gemspec glob: RUBY_PLATFORM == "java" ? "upsert-java.gemspec" : "upsert.gemspec"
else
  gemspec name: RUBY_PLATFORM == "java" ? "upsert-java" : "upsert"
end

case RUBY_PLATFORM
when "java"
  gem "ffi", platforms: :jruby
else
  gem "ffi"
end

group "test" do
  gem "testmetrics_rspec"
end