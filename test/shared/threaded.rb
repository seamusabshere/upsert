shared_examples_for 'is thread-safe' do
  it "is safe to use one-by-one" do
    upsert = Upsert.new connection, :pets
    assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
      ts = []
      10.times do
        ts << Thread.new do
          sleep 0.2
          upsert.row({:name => 'Jerry'}, :gender => 'male')
          upsert.row({:name => 'Jerry'}, :gender => 'neutered')
        end
        ts.each { |t| t.join }
      end
    end
  end
  it "is safe to use batch" do
    assert_creates(Pet, [{:name => 'Jerry', :gender => 'neutered'}]) do
      Upsert.batch(connection, :pets) do |upsert|
        ts = []
        10.times do
          ts << Thread.new do
            sleep 0.2
            upsert.row({:name => 'Jerry'}, :gender => 'male')
            upsert.row({:name => 'Jerry'}, :gender => 'neutered')
          end
          ts.each { |t| t.join }
        end
      end
    end
  end
end
