source 'https://rubygems.org'

# Specify your gem's dependencies in upsert.gemspec

gemspec name: RUBY_PLATFORM == "java" ? "upsert-java" : "upsert"

case RUBY_PLATFORM
when "java"
  gem "ffi", platforms: :jruby
else
  gem "ffi"
end