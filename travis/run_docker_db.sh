case "$DB" in
  postgresql)
      docker run --tmpfs /var/lib/postgresql/data:rw --rm --name db_server \
        -e POSTGRES_USER=$DB_USER -e POSTGRES_PASSWORD=$DB_PASSWORD -e POSTGRES_DB=$DB_NAME
        -p 5432:5432 -d \
        $DB_VERSION
    ;;
  mysql)
      docker run --tmpfs /var/lib/mysq --rm --name db_server \
        -e MYSQL_ROOT_PASSWORD=root -e MYSQL_USER=$DB_USER -e MYSQL_PASSWORD=$DB_PASSWORD -e MYSQL_DATABASE=$DB_NAME \
        -p 3306:3306 -d \
        $DB_VERSION \
        --character-set-server=utf8mb4 --collation-server=utf8mb4_unicode_ci
    ;;
esac