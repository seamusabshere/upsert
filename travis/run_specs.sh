if [ "$(rvm info | grep jruby | wc -l)" -gt 5 ]; then
  export CLASSPATH="$(bundle show jdbc-postgres)/lib/postgresql-42.1.4.jar"`;
fi
bundle exec rake spec
