#!/usr/bin/env bash

if [ "$(rvm info | grep jruby | wc -l)" -gt 5 ]; then
  export CLASSPATH="$(bundle show jdbc-postgres)/lib/postgresql-42.1.4.jar";
fi
# https://markmail.org/message/qb32j3ybhhu7bfge#query:+page:1+mid:qb32j3ybhhu7bfge+state:results
bundle exec rake spec
