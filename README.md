# Upsert

Finally, all those SQL MERGE tricks codified.

## Supported databases

### MySQL

    # http://dev.mysql.com/doc/refman/5.0/en/insert-on-duplicate.html
    INSERT INTO table (a,b,c) VALUES (1,2,3)
      ON DUPLICATE KEY UPDATE c=c+1;

### PostgreSQL

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

    # http://stackoverflow.com/questions/1109061/insert-on-duplicate-update-postgresql
    UPDATE table SET field='C', field2='Z' WHERE id=3;
    INSERT INTO table (id, field, field2)
      SELECT 3, 'C', 'Z'
      WHERE NOT EXISTS (SELECT 1 FROM table WHERE id=3);


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

    # http://stackoverflow.com/questions/2717590/sqlite-upsert-on-duplicate-key-update
    INSERT OR IGNORE INTO visits VALUES ($ip, 0);
    UPDATE visits SET hits = hits + 1 WHERE ip LIKE $ip;
