class Upsert
  class Connection
    # @private
    class Mysql2_Client < Connection
      def execute(sql)
        Upsert.logger.debug { %([upsert] #{sql}) }
        if results = metal.query(sql)
          rows = []
          results.each { |row| rows << row }
          if rows[0].is_a? Array
            # you don't know if mysql2 is going to give you an array or a hash... and you shouldn't specify, because it's sticky
            fields = results.fields
            rows.map { |row| Hash[fields.zip(row)] }
          else
            rows
          end
        end
      end

      def quote_value(v)
        case v
        when NilClass
          NULL_WORD
        when Upsert::Binary
          quote_binary v.value
        when String
          quote_string v
        when TrueClass, FalseClass
          quote_boolean v
        when BigDecimal
          quote_big_decimal v
        when Numeric
          v
        when Symbol
          quote_string v.to_s
        when DateTime, Time
          # mysql doesn't like it when you send timezone to a datetime
          quote_string Upsert.utc_iso8601(v, false)
        when Date
          quote_date v
        else
          raise "not sure how to quote #{v.class}: #{v.inspect}"
        end
      end

      def quote_boolean(v)
        v ? "TRUE" : "FALSE"
      end

      def quote_string(v)
        SINGLE_QUOTE + metal.escape(v) + SINGLE_QUOTE
      end

      # This doubles the size of the representation.
      def quote_binary(v)
        X_AND_SINGLE_QUOTE + v.unpack("H*")[0] + SINGLE_QUOTE
      end

      # put raw binary straight into sql
      # might work if we could get the encoding issues fixed when joining together the values for the sql
      # alias_method :quote_binary, :quote_string

      def quote_date(v)
        quote_string v.strftime(ISO8601_DATE)
      end

      def quote_ident(k)
        BACKTICK + metal.escape(k.to_s) + BACKTICK
      end

      def quote_big_decimal(v)
        v.to_s("F")
      end
    end
  end
end
