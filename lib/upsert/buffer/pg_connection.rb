require 'upsert/buffer/pg_connection/merge_function'

class Upsert
  class Buffer
    # @private
    class PG_Connection < Buffer
      def ready
        return if rows.empty?
        row = rows.shift
        MergeFunction.execute(self, row)
      end

      def clear_database_functions
        connection = parent.connection
        # http://stackoverflow.com/questions/7622908/postgresql-drop-function-without-knowing-the-number-type-of-parameters
        connection.execute <<-EOS
CREATE OR REPLACE FUNCTION pg_temp.upsert_delfunc(text)
  RETURNS void AS
$BODY$
DECLARE
   _sql text;
BEGIN

FOR _sql IN
   SELECT 'DROP FUNCTION ' || quote_ident(n.nspname)
                    || '.' || quote_ident(p.proname)
           || '(' || pg_catalog.pg_get_function_identity_arguments(p.oid) || ');'
   FROM   pg_catalog.pg_proc p
   LEFT   JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE  p.proname = $1
   AND    pg_catalog.pg_function_is_visible(p.oid) -- you may or may not want this
LOOP
   EXECUTE _sql;
END LOOP;

END;
$BODY$
  LANGUAGE plpgsql;
EOS
        res = connection.execute(%{SELECT proname FROM pg_proc WHERE proname LIKE 'upsert_%'})
        res.each do |row|
          k = row['proname']
          next if k == 'upsert_delfunc'
          Upsert.logger.info %{[upsert] Dropping function #{k.inspect}}
          connection.execute %{SELECT pg_temp.upsert_delfunc('#{k}')}
        end
      end
    end
  end
end
