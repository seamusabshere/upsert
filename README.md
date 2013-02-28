# Upsert

Make it easy to upsert on traditional RDBMS like MySQL, PostgreSQL, and SQLite3&mdash;hey look NoSQL!. Transparently creates (and re-uses) stored procedures/functions when necessary.

You pass it a bare-metal connection to the database like `Mysql2::Client` (from `mysql2` gem on MRI) or `Java::OrgPostgresqlJdbc4::Jdbc4Connection` (from `jdbc-postgres` on Jruby).

As databases start to natively support SQL MERGE (which is basically upsert), this library will take advantage (but you won't have to change your code).

Does **not** depend on ActiveRecord.

70&ndash;90%+ faster than emulating upsert with ActiveRecord.

Supports MRI and JRuby.

## Usage

You pass a __selector__ that uniquely identifies a row, whether it exists or not. You also pass a __setter__, attributes that should be set on that row.

Syntax inspired by [mongo-ruby-driver's update method](http://api.mongodb.org/ruby/1.6.4/Mongo/Collection.html#update-instance_method).

### Basic
    
```ruby
connection = Mysql2::Client.new([...])
table_name = :pets
upsert = Upsert.new connection, table_name
# N times...
upsert.row({:name => 'Jerry'}, :breed => 'beagle', :created_at => Time.now)
```

The `created_at` and `created_on` columns are used for inserts, but ignored on updates.

So just to reiterate you've got a __selector__ and a __setter__:

```ruby
selector = { :name => 'Jerry' }
setter = { :breed => 'beagle' }
upsert.row(selector, setter)
```

### Batch mode

By organizing your upserts into a batch, we can do work behind the scenes to make them faster.

```ruby
connection = Mysql2::Client.new([...])
Upsert.batch(connection, :pets) do |upsert|
  # N times...
  upsert.row({:name => 'Jerry'}, :breed => 'beagle')
  upsert.row({:name => 'Pierre'}, :breed => 'tabby')
end
```

Batch mode is tested to be about 80% faster on PostgreSQL, MySQL, and SQLite3 than other ways to emulate upsert (see the tests, which fail if they are not faster).

### ActiveRecord helper method

```ruby
require 'upsert/active_record_upsert'
# N times...
Pet.upsert({:name => 'Jerry'}, :breed => 'beagle')
```

## Wishlist

Pull requests for any of these would be greatly appreciated:

1. Cache JDBC PreparedStatement objects.
1. Allow "true" upserting like `upsert.row({name: 'Jerry'}, counter: Upsert.sql('counter+1'))`
1. Sanity check my three benchmarks (four if you include activerecord-import on MySQL). Do they accurately represent optimized alternatives?
1. Provide `require 'upsert/debug'` that will make sure you are selecting on columns that have unique indexes
1. Test that `Upsert` instances accept arbitrary columns, even within a batch, which is what people probably expect.

## Real-world usage

<p><a href="http://brighterplanet.com"><img src="https://s3.amazonaws.com/static.brighterplanet.com/assets/logos/flush-left/inline/green/rasterized/brighter_planet-160-transparent.png" alt="Brighter Planet logo"/></a></p>

We use `upsert` for [big data processing at Brighter Planet](http://brighterplanet.com/research) and in production at

* [Brighter Planet's impact estimate web service](http://impact.brighterplanet.com)
* [Brighter Planet's reference data web service](http://data.brighterplanet.com)

Originally written to speed up the [`data_miner`](https://github.com/seamusabshere/data_miner) data mining library.

## Supported databases/drivers

<table>
  <tr>
    <th>*</th>
    <th>MySQL</th>
    <th>PostgreSQL</th>
    <th>SQLite3</th>
  </tr>
  <tr>
    <th>MRI</th>
    <td><a href="https://rubygems.org/gems/mysql2">mysql2</a></td>
    <td><a href="https://rubygems.org/gems/pg">pg</a></td>
    <td><a href="https://rubygems.org/gems/sqlite3">sqlite3</a></td>
  </tr>
  <tr>
    <th>JRuby</th>
    <td><a href="https://rubygems.org/gems/jdbc-mysql">jdbc-mysql</a></td>
    <td><a href="https://rubygems.org/gems/jdbc-postgres">jdbc-postgres</a></td>
    <td><a href="https://rubygems.org/gems/jdbc-sqlite3">jdbc-sqlite3</a></td>
  </tr>
</table>

See below for details about what SQL MERGE trick (emulation of upsert) is used, performance, code examples, etc.

### Rails / ActiveRecord

(assuming that one of the other three supported drivers is being used under the covers)

```ruby
Upsert.new Pet.connection, Pet.table_name
```

#### Speed

Depends on the driver being used!

#### SQL MERGE trick

Depends on the driver being used!

### MySQL

On MRI, use the [mysql2](https://rubygems.org/gems/mysql2) driver.

```ruby
require 'mysql2'
connection = Mysql2::Connection.new(:username => 'root', :password => 'password', :database => 'upsert_test')
table_name = :pets
upsert = Upsert.new(connection, table_name)
```

On JRuby, use the [jdbc-mysql](https://rubygems.org/gems/jdbc-mysql) driver.

```ruby
require 'jdbc/mysql'
java.sql.DriverManager.register_driver com.mysql.jdbc.Driver.new
connection = java.sql.DriverManager.get_connection "jdbc:mysql://127.0.0.1/mydatabase?user=root&password=password"
```

#### Speed

From the tests (updated 11/7/12):

    Upsert was 82% faster than find + new/set/save
    Upsert was 85% faster than find_or_create + update_attributes
    Upsert was 90% faster than create + rescue/find/update
    Upsert was 46% faster than faking upserts with activerecord-import

#### SQL MERGE trick

Thanks to [Dennis Hennen's StackOverflow response!](http://stackoverflow.com/questions/11371479/how-to-translate-postgresql-merge-db-aka-upsert-function-into-mysql/)!

```sql
CREATE PROCEDURE upsert_pets_SEL_name_A_tag_number_SET_name_A_tag_number(`name_sel` varchar(255), `tag_number_sel` int(11), `name_set` varchar(255), `tag_number_set` int(11))
BEGIN
  DECLARE done BOOLEAN;
  REPEAT
    BEGIN
      -- If there is a unique key constraint error then 
      -- someone made a concurrent insert. Reset the sentinel
      -- and try again.
      DECLARE ER_DUP_UNIQUE CONDITION FOR 23000;
      DECLARE ER_INTEG CONDITION FOR 1062;
      DECLARE CONTINUE HANDLER FOR ER_DUP_UNIQUE BEGIN
        SET done = FALSE;
      END;
      
      DECLARE CONTINUE HANDLER FOR ER_INTEG BEGIN
        SET done = TRUE;
      END;

      SET done = TRUE;
      SELECT COUNT(*) INTO @count FROM `pets` WHERE `name` = `name_sel` AND `tag_number` = `tag_number_sel`;
      -- Race condition here. If a concurrent INSERT is made after
      -- the SELECT but before the INSERT below we'll get a duplicate
      -- key error. But the handler above will take care of that.
      IF @count > 0 THEN 
        -- UPDATE table_name SET b = b_SET WHERE a = a_SEL;
        UPDATE `pets` SET `name` = `name_set`, `tag_number` = `tag_number_set` WHERE `name` = `name_sel` AND `tag_number` = `tag_number_sel`;
      ELSE
        -- INSERT INTO table_name (a, b) VALUES (k, data);
        INSERT INTO `pets` (`name`, `tag_number`) VALUES (`name_set`, `tag_number_set`);
      END IF;
    END;
  UNTIL done END REPEAT;
END
```

### PostgreSQL

On MRI, use the [pg](https://rubygems.org/gems/pg) driver.

```ruby
require 'pg'
connection = PG.connect(:dbname => 'upsert_test')
table_name = :pets
upsert = Upsert.new(connection, table_name)
```

On JRuby, use the [jdbc-postgres](https://rubygems.org/gems/jdbc-postgres) driver.

```ruby
require 'jdbc/postgres'
java.sql.DriverManager.register_driver org.postgresql.Driver.new
connection = java.sql.DriverManager.get_connection "jdbc:postgresql://127.0.0.1/mydatabase?user=root&password=password"
```

If you want to use HStore, make the `pg-hstore` gem available and pass a Hash in setters:

```ruby
gem 'pg-hstore'
require 'pg_hstore'
upsert.row({:name => 'Bill'}, :mydata => {:a => 1, :b => 2})
```

#### Speed

From the tests (updated 9/21/12):

    Upsert was 72% faster than find + new/set/save
    Upsert was 79% faster than find_or_create + update_attributes
    Upsert was 83% faster than create + rescue/find/update
    # (can't compare to activerecord-import because you can't fake it on pg)

#### SQL MERGE trick

Adapted from the [canonical PostgreSQL upsert example](http://www.postgresql.org/docs/current/interactive/plpgsql-control-structures.html#PLPGSQL-ERROR-TRAPPING):

```sql
CREATE OR REPLACE FUNCTION upsert_pets_SEL_name_A_tag_number_SET_name_A_tag_number("name_sel" character varying(255), "tag_number_sel" integer, "name_set" character varying(255), "tag_number_set" integer) RETURNS VOID AS
$$
DECLARE
  first_try INTEGER := 1;
BEGIN
  LOOP
    -- first try to update the key
    UPDATE "pets" SET "name" = "name_set", "tag_number" = "tag_number_set"
      WHERE "name" = "name_sel" AND "tag_number" = "tag_number_sel";
    IF found THEN
      RETURN;
    END IF;
    -- not there, so try to insert the key
    -- if someone else inserts the same key concurrently,
    -- we could get a unique-key failure
    BEGIN
      INSERT INTO "pets"("name", "tag_number") VALUES ("name_set", "tag_number_set");
      RETURN;
    EXCEPTION WHEN unique_violation THEN
      -- seamusabshere 9/20/12 only retry once
      IF (first_try = 1) THEN
        first_try := 0;
      ELSE
        RETURN;
      END IF;
      -- Do nothing, and loop to try the UPDATE again.
    END;
  END LOOP;
END;
$$
LANGUAGE plpgsql;
```

I slightly modified it so that it only retries once - don't want infinite loops.

### Sqlite

On MRI, use the [sqlite3](https://rubygems.org/gems/sqlite3) driver.

```ruby
require 'sqlite3'
connection = SQLite3::Database.open(':memory:')
table_name = :pets
upsert = Upsert.new(connection, table_name)
```

On JRuby, use the [jdbc-sqlite3](https://rubygems.org/gems/jdbc-sqlite3) driver.

```ruby
# TODO somebody please verify
require 'jdbc/sqlite3'
java.sql.DriverManager.register_driver org.sqlite.Driver.new
connection = java.sql.DriverManager.get_connection "jdbc:sqlite://127.0.0.1/mydatabase?user=root&password=password"
```

#### Speed

From the tests (updated 9/21/12):

    Upsert was 77% faster than find + new/set/save
    Upsert was 80% faster than find_or_create + update_attributes
    Upsert was 85% faster than create + rescue/find/update
    # (can't compare to activerecord-import because you can't fake it on sqlite3)

#### SQL MERGE trick

Thanks to [@dan04's answer on StackOverflow](http://stackoverflow.com/questions/2717590/sqlite-upsert-on-duplicate-key-update):

```sql
INSERT OR IGNORE INTO visits VALUES (127.0.0.1, 1);
UPDATE visits SET visits = 1 WHERE ip LIKE 127.0.0.1;
```

## Features

### Tested to be fast and portable

In addition to correctness, the library's tests check that it is

1. Faster than comparable upsert techniques
2. Compatible with supported databases

### Not dependent on ActiveRecord

As below, all you need is a raw database connection like a `Mysql2::Connection`, `PG::Connection` or a `SQLite3::Database`. These are equivalent:

```ruby
# with activerecord
Upsert.new ActiveRecord::Base.connection, :pets
# with activerecord, prettier
Upsert.new Pet.connection, Pet.table_name
# without activerecord
Upsert.new Mysql2::Connection.new([...]), :pets
```

### For a specific use case, faster and more portable than `activerecord-import`

You could also use [activerecord-import](https://github.com/zdennis/activerecord-import) to upsert:

```ruby
Pet.import columns, all_values, :timestamps => false, :on_duplicate_key_update => columns
```

`activerecord-import`, however, only works on MySQL and requires ActiveRecord&mdash;and if all you are doing is upserts, `upsert` is tested to be 40% faster. And you don't have to put all of the rows to be upserted into a single huge array - you can batch them using `Upsert.batch`.

## Gotchas

### No automatic typecasting beyond what the adapter/driver provides

We don't have any logic to convert integers into strings, strings into integers, etc. in order to satisfy PostgreSQL/etc.'s strictness on this issue.

So if you try to upsert a blank string (`''`) into an integer field in PostgreSQL, you will get an error.

### Dates and times are converted to UTC

Datetimes are immediately converted to UTC and sent to the database as ISO8601 strings.

If you're using MySQL, make sure server/connection timezone is UTC. If you're using Rails and/or ActiveRecord, you might want to check `ActiveRecord::Base.default_timezone`... it should probably be `:utc`.

In general, run some upserts and make sure datetimes get persisted like you expect.

## Copyright

Copyright 2012 Seamus Abshere

