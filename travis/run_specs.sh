#!/usr/bin/env bash

# https://markmail.org/message/qb32j3ybhhu7bfge#query:+page:1+mid:qb32j3ybhhu7bfge+state:results
if [ "$(rvm info | grep jruby | wc -l)" -gt 5 ]; then
  export CLASSPATH="$(bundle show jdbc-postgres)/lib/postgresql-42.1.4.jar:$CLASSPATH"
  export CLASSPATH="$(bundle show jdbc-mysql)/lib/mysql-connector-java-5.1.47-bin.jar:$CLASSPATH";
  export CLASSPATH="$(bundle show jdbc-sqlite3)/lib/sqlite-jdbc-3.20.1.jar:$CLASSPATH";
fi
bundle exec rake spec
