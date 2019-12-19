source 'https://rubygems.org'

# Specify your gem's dependencies in upsert.gemspec

gemspec glob: RUBY_PLATFORM == "java" ? "upsert-java.gemspec" : "upsert.gemspec"

case RUBY_PLATFORM
when "java"
  gem "ffi", platforms: :jruby
else
  gem "ffi"
end

group "test" do
  gem "testmetrics_rspec"
end