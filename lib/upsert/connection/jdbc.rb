class Upsert
  class Connection
    # @private
    module Jdbc
      # /Users/seamusabshere/.rvm/gems/jruby-head/gems/activerecord-jdbc-adapter-1.2.2.1/src/java/arjdbc/jdbc/RubyJdbcConnection.java
      GETTER = {
        java.sql.Types::VARCHAR     => 'getString',
        java.sql.Types::OTHER       => 'getString', # ?! i guess unicode text?
        java.sql.Types::BINARY      => 'getBlob',
        java.sql.Types::LONGVARCHAR => 'getString',
        java.sql.Types::BIGINT      => 'getLong',
        java.sql.Types::INTEGER     => 'getInt',
      }
      java.sql.Types.constants.each do |type_name|
        i = java.sql.Types.const_get type_name
        unless GETTER.has_key?(i)
          GETTER[i] = 'get' + type_name[0].upcase + type_name[1..-1].downcase
        end
      end
      SETTER = Hash.new do |hash, k|
        hash[k] = 'set' + k
      end.merge(
        'TrueClass'  => 'setBoolean',
        'FalseClass' => 'setBoolean',
        'Fixnum'     => 'setInt',
      )

      def binary(v)
        v.value.to_java_bytes.java_object
      end

      def execute(sql, params = nil)
        has_result = if params
          Upsert.logger.debug { %{[upsert] #{sql} with #{params.inspect}} }
          setters = self.class.const_get(:SETTER)
          statement = metal.prepareStatement sql
          params.each_with_index do |v, i|
            if v.is_a?(Fixnum) && v > 2_147_483_647
              statement.setLong i+1, v
              next
            end

            case v
            when Upsert::Binary
              statement.setBytes i+1, binary(v)
            when BigDecimal
              statement.setBigDecimal i+1, java.math.BigDecimal.new(v.to_s)
            when NilClass
              # http://stackoverflow.com/questions/4243513/why-does-preparedstatement-setnull-requires-sqltype
              statement.setObject i+1, nil
            else
              setter = setters[v.class.name]
              statement.send setter, i+1, v
            end
          end
          statement.execute
        else
          Upsert.logger.debug { %{[upsert] #{sql}} }
          statement = metal.createStatement
          statement.execute sql
        end
        if not has_result
          statement.close
          return
        end
        getters = self.class.const_get(:GETTER)
        raw_result = statement.getResultSet
        meta = raw_result.getMetaData
        count = meta.getColumnCount
        column_name_and_getter = (1..count).inject({}) do |memo, i|
          memo[i] = [ meta.getColumnName(i), getters[meta.getColumnType(i)] ]
          memo
        end
        result = []
        while raw_result.next
          row = {}
          column_name_and_getter.each do |i, cg|
            column_name, getter = cg
            if getter == 'getNull'
              row[column_name] = nil
            else
              row[column_name] = raw_result.send(getter, i)
            end
          end
          result << row
        end
        statement.close
        result
      end
    end
  end
end
