source 'https://rubygems.org'

# Specify your gem's dependencies in upsert.gemspec

gemspec name: RUBY_VERSION == "java" ? "upsert-java" : "upsert"

case RUBY_VERSION
when "java"
  gem "ffi", platforms: :jruby
else
  gem "ffi"
end