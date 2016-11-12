#!/usr/bin/env bash
# Ref https://github.com/travis-ci/travis-ci/issues/2250
# Ref https://github.com/jruby/activerecord-jdbc-adapter/issues/481
# JDBC adapters determine the encoding automatically via the server's charset and collation
echo "Tuning MySQL"
echo -e "[server]\ncharacter-set-server=utf8mb4\ncollation-server=utf8mb4_unicode_ci\n" | sudo tee -a /etc/mysql/my.cnf
sudo service mysql restart
