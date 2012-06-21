# Upsert

Finally, all those SQL MERGE tricks codified so that you can do "upsert" on MySQL, PostgreSQL, and Sqlite.

## Usage

Let's say you have...

    class Pet < ActiveRecord::Base
      # col :name
      # col :breed
    end

### One at a time

    upsert = Upsert.new Pet.connection, Pet.table_name
    selector = {:name => 'Jerry'}
    document = {:breed => 'beagle'}
    upsert.row selector, document

### Multiple upserts bundled together for speed

    Upsert.stream(Pet.connection, Pet.table_name) do |upsert|
      # [...]
      upsert.row({:name => 'Jerry'}, :breed => 'beagle')
      # [...]
      upsert.row({:name => 'Pierre'}, :breed => 'tabby')
      # [...]
    end

Rows are buffered in memory until it's efficient to send them to the database.

## Real-world usage

<p><a href="http://brighterplanet.com"><img src="https://s3.amazonaws.com/static.brighterplanet.com/assets/logos/flush-left/inline/green/rasterized/brighter_planet-160-transparent.png" alt="Brighter Planet logo"/></a></p>

We use `upsert` for [big data processing at Brighter Planet](http://brighterplanet.com/research) and in production at

* [Brighter Planet's impact estimate web service](http://impact.brighterplanet.com)
* [Brighter Planet's reference data web service](http://data.brighterplanet.com)

Originally written to speed up the [`data_miner`](https://github.com/seamusabshere/data_miner) data mining library.

## Supported databases

### MySQL

Using the [mysql2](https://rubygems.org/gems/mysql2) driver.

    Upsert.new Mysql2::Connection.new([...]), :pets

#### Speed

From the tests:

    Upsert was 77% faster than find + new/set/save
    Upsert was 84% faster than create + rescue/find/update
    Upsert was 82% faster than find_or_create + update_attributes
    Upsert was 47% faster than faking upserts with activerecord-import

#### SQL MERGE trick

"ON DUPLICATE KEY UPDATE" where we just set everything to the value of the insert.

    # http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html
    INSERT INTO table (a,b,c) VALUES (1,2,3), (4,5,6)
      ON DUPLICATE KEY UPDATE a=VALUES(a),b=VALUES(b),c=VALUES(c);

Since this is an upsert helper library, not a general-use ON DUPLICATE KEY UPDATE wrapper, you **can't** do things like `c=c+1`.

### PostgreSQL

Using the [pg](https://rubygems.org/gems/pg) driver.

    Upsert.new PG.connect([...]), :pets

#### Speed

From the tests:

    Upsert was 73% faster than find + new/set/save
    Upsert was 84% faster than find_or_create + update_attributes
    Upsert was 87% faster than create + rescue/find/update
    # (can't compare to activerecord-import because you can't fake it on pg)

#### SQL MERGE trick

    # http://www.postgresql.org/docs/current/interactive/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING
    CREATE TABLE db (a INT PRIMARY KEY, b TEXT);
    CREATE FUNCTION merge_db(key INT, data TEXT) RETURNS VOID AS
    $$
    BEGIN
        LOOP
            -- first try to update the key
            UPDATE db SET b = data WHERE a = key;
            IF found THEN
                RETURN;
            END IF;
            -- not there, so try to insert the key
            -- if someone else inserts the same key concurrently,
            -- we could get a unique-key failure
            BEGIN
                INSERT INTO db(a,b) VALUES (key, data);
                RETURN;
            EXCEPTION WHEN unique_violation THEN
                -- Do nothing, and loop to try the UPDATE again.
            END;
        END LOOP;
    END;
    $$
    LANGUAGE plpgsql;
    SELECT merge_db(1, 'david');
    SELECT merge_db(1, 'dennis');

The decision was made **not** to use the following because it's not straight from the manual:

    # http://stackoverflow.com/questions/1109061/insert-on-duplicate-update-postgresql
    UPDATE table SET field='C', field2='Z' WHERE id=3;
    INSERT INTO table (id, field, field2)
      SELECT 3, 'C', 'Z'
      WHERE NOT EXISTS (SELECT 1 FROM table WHERE id=3);

This was also rejected because there's something we can use in the manual:

    # http://stackoverflow.com/questions/5269590/why-doesnt-this-rule-prevent-duplicate-key-violations
    BEGIN;
    CREATE TEMP TABLE stage_data(key_column, data_columns...) ON COMMIT DROP;
    \copy stage_data from data.csv with csv header
    -- prevent any other updates while we are merging input (omit this if you don't need it)
    LOCK target_data IN SHARE ROW EXCLUSIVE MODE;
    -- insert into target table
    INSERT INTO target_data(key_column, data_columns...)
       SELECT key_column, data_columns...
       FROM stage_data
       WHERE NOT EXISTS (SELECT 1 FROM target_data
                         WHERE target_data.key_column = stage_data.key_column)
    END;

### Sqlite

Using the [sqlite3](https://rubygems.org/gems/sqlite3) driver.

    Upsert.new SQLite3::Database.open([...]), :pets

#### Speed

FIXME tests are segfaulting. Pull request would be lovely.

#### SQL MERGE trick

    # http://stackoverflow.com/questions/2717590/sqlite-upsert-on-duplicate-key-update
    # bad example because we're not doing on-duplicate-key update
    INSERT OR IGNORE INTO visits VALUES (127.0.0.1, 1);
    UPDATE visits SET visits = 1 WHERE ip LIKE 127.0.0.1;

### Rails / ActiveRecord

(assuming that one of the other three supported drivers is being used under the covers)

    Upsert.new Pet.connection, Pet.table_name

#### Speed

Depends on the driver being used!

#### SQL MERGE trick

Depends on the driver being used!

## Features

### Tested to be fast and portable

In addition to correctness, the library's tests check that it is

1. Faster than comparable upsert techniques
2. Compatible with supported databases

### Not dependent on ActiveRecord

As below, all you need is a raw database connection like a `Mysql2::Connection`, `PG::Connection` or a `SQLite3::Database`. These are equivalent:

    # with activerecord
    Upsert.new ActiveRecord::Base.connection, :pets
    # with activerecord, prettier
    Upsert.new Pet.connection, Pet.table_name
    # without activerecord
    Upsert.new Mysql2::Connection.new([...]), :pets

### For a specific use case, faster and more portable than `activerecord-import`

You could also use [activerecord-import](https://github.com/zdennis/activerecord-import) to upsert:

    Pet.import columns, all_values, :timestamps => false, :on_duplicate_key_update => columns

This, however, only works on MySQL and requires ActiveRecord&mdash;and if all you are doing is upserts, `upsert` is tested to be 40% faster. And you don't have to put all of the rows to be upserted into a single huge array - you can stream them using `Upsert.stream`.

### Loosely based on mongo-ruby-driver's upsert functionality

The `selector` and `document` arguments are inspired by the upsert functionality of the [mongo-ruby-driver's update method](http://api.mongodb.org/ruby/1.6.4/Mongo/Collection.html#update-instance_method).

## Wishlist

1. `Pet.upsert`... duh

## Copyright

Copyright 2012 Brighter Planet, Inc.

