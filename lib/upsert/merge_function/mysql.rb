class Upsert
  class MergeFunction
    # @private
    module Mysql
      def self.included(klass)
        klass.extend ClassMethods
      end

      module ClassMethods
        # http://stackoverflow.com/questions/733349/list-of-stored-procedures-functions-mysql-command-line
        def clear(connection)
          connection.execute("SHOW PROCEDURE STATUS WHERE Db = DATABASE() AND Name LIKE '#{MergeFunction::NAME_PREFIX}%'").map do |row|
            row['Name'] || row['ROUTINE_NAME']
          end.each do |name|
            connection.execute "DROP PROCEDURE IF EXISTS #{connection.quote_ident(name)}"
          end
        end
      end

      # http://stackoverflow.com/questions/11371479/how-to-translate-postgresql-merge-db-aka-upsert-function-into-mysql/
      def create!
        Upsert.logger.info "[upsert] Creating or replacing database function #{name.inspect} on table #{table_name.inspect} for selector #{selector_keys.map(&:inspect).join(', ')} and setter #{setter_keys.map(&:inspect).join(', ')}"
        selector_column_definitions = column_definitions.select { |cd| selector_keys.include?(cd.name) }
        setter_column_definitions = column_definitions.select { |cd| setter_keys.include?(cd.name) }
        update_column_definitions = setter_column_definitions.select { |cd| cd.name !~ CREATED_COL_REGEX }
        quoted_name = connection.quote_ident name
        connection.execute "DROP PROCEDURE IF EXISTS #{quoted_name}"
        connection.execute(%{
          CREATE PROCEDURE #{quoted_name}(#{(selector_column_definitions.map(&:to_selector_arg) + setter_column_definitions.map(&:to_setter_arg)).join(', ')})
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
                SELECT COUNT(*) INTO @count FROM #{quoted_table_name} WHERE #{selector_column_definitions.map(&:to_selector).join(' AND ')};
                -- Race condition here. If a concurrent INSERT is made after
                -- the SELECT but before the INSERT below we'll get a duplicate
                -- key error. But the handler above will take care of that.
                IF @count > 0 THEN 
                  -- UPDATE table_name SET b = b_SET WHERE a = a_SEL;
                  UPDATE #{quoted_table_name} SET #{update_column_definitions.map(&:to_setter).join(', ')} WHERE #{selector_column_definitions.map(&:to_selector).join(' AND ')};
                ELSE
                  -- INSERT INTO table_name (a, b) VALUES (k, data);
                  INSERT INTO #{quoted_table_name} (#{setter_column_definitions.map(&:quoted_name).join(', ')}) VALUES (#{setter_column_definitions.map(&:to_setter_value).join(', ')});
                END IF;
              END;
            UNTIL done END REPEAT;
          END
        })
      end
    end
  end
end
