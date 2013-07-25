class Upsert
  class Connection
    # @private
    module Postgresql
      def bind_value(v)
        case v
        when Array
          # pg array escaping lifted from https://github.com/tlconnor/activerecord-postgres-array/blob/master/lib/activerecord-postgres-array/array.rb
          '{' + v.map do |vv|
            vv = vv.to_s.dup
            vv.gsub! /\\/, '\&\&'
            vv.gsub! /'/, "''"
            vv.gsub! /"/, '\"'
            %{"#{vv}"}
          end.join(',') + '}'
        when Hash
          # you must require 'pg_hstore' from the 'pg-hstore' gem yourself
          ::PgHstore.dump v, true
        else
          super
        end
      end
    end
  end
end
