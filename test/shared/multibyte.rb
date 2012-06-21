# -*- encoding: utf-8 -*-
shared_examples_for "supports multibyte" do
  it "works one-by-one" do
    assert_creates(Pet, [{:name => 'I♥NY', :gender => 'périferôl'}]) do
      upsert = Upsert.new connection, :pets
      upsert.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
    end
  end
  it "works serially" do
    assert_creates(Pet, [{:name => 'I♥NY', :gender => 'jÚrgen'}]) do
      upsert = Upsert.new connection, :pets
      upsert.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
      upsert.row({:name => 'I♥NY'}, {:gender => 'jÚrgen'})
    end
  end
  it "works streaming" do
    assert_creates(Pet, [{:name => 'I♥NY', :gender => 'jÚrgen'}]) do
      Upsert.stream(connection, :pets) do |upsert|
        upsert.row({:name => 'I♥NY'}, {:gender => 'périferôl'})
        upsert.row({:name => 'I♥NY'}, {:gender => 'jÚrgen'})
      end
    end
  end
  it "won't overflow" do
    upsert = Upsert.new connection, :pets
    if upsert.respond_to?(:max_sql_bytesize)
      max = upsert.send(:max_sql_bytesize)
      ticks = max / 3 - 2
      lambda do
        loop do
          upsert.row({:name => 'Jerry'}, :home_address => ("日" * ticks))
          ticks += 1
        end
      end.must_raise Upsert::TooBig
    end
  end
end
