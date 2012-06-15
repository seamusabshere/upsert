class Upsert
  # A wrapper class for binary strings so that Upsert knows to escape them as such.
  #
  # Create them with +Upsert.binary(x)+
  class Binary < ::String
  end
end
