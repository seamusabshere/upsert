docker network create -d bridge --subnet 	172.25.0.0/16 --gateway 172.25.0.1 upsert_test

case "$DB" in
  postgresql)
      docker run --tmpfs /var/lib/postgresql/data:rw --rm --name upsert_test_db_server \
        -e POSTGRES_USER=$DB_USER -e POSTGRES_PASSWORD=$DB_PASSWORD -e POSTGRES_DB=$DB_NAME \
        -p 5432:5432 -d \
        $DB_VERSION
      sleep 10
    ;;
  mysql)
      docker run --network upsert_test --ip 172.25.0.2 --tmpfs /var/lib/mysql:rw --rm --name upsert_test_db_server \
        -e MYSQL_ROOT_PASSWORD=root -e MYSQL_USER=$DB_USER -e MYSQL_PASSWORD=$DB_PASSWORD -e MYSQL_DATABASE=$DB_NAME \
        -p 3306:3306 -d \
        $DB_VERSION \
        --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci --default-authentication-plugin=mysql_native_password --default-time-zone=+00:00
      sleep 10
      docker run --network upsert_test --rm $DB_VERSION mysql -h172.25.0.2 -uroot -proot -e "GRANT ALL ON *.* TO '$DB_USER'"
    ;;
esac