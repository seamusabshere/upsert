# Ref https://github.com/clkao/plv8x/blob/master/.travis.yml
echo "Install postgres version $PGVERSION"
psql --version
sudo /etc/init.d/postgresql stop
sudo apt-get -y --purge remove postgresql libpq-dev libpq5 postgresql-client-common postgresql-common
sudo rm -rf /var/lib/postgresql
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo sh -c "echo deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main $PGVERSION >> /etc/apt/sources.list.d/postgresql.list"
sudo sh -c "echo deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg-testing main $PGVERSION >> /etc/apt/sources.list.d/postgresql.list"
sudo apt-get update -qq
sudo apt-get -y -o Dpkg::Options::=--force-confdef -o Dpkg::Options::="--force-confnew" install postgresql-$PGVERSION postgresql-server-dev-$PGVERSION postgresql-$PGVERSION-plv8
sudo chmod 777 /etc/postgresql/$PGVERSION/main/pg_hba.conf
sudo echo "local   all         postgres                          trust" > /etc/postgresql/$PGVERSION/main/pg_hba.conf
sudo echo "local   all         all                               trust" >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
sudo echo "host    all         all         127.0.0.1/32          trust" >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
sudo echo "host    all         all         ::1/128               trust" >> /etc/postgresql/$PGVERSION/main/pg_hba.conf
sudo /etc/init.d/postgresql restart
