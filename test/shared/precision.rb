shared_examples_for 'is precise' do
  it "stores small numbers precisely" do
    small = -0.00000000634943
    upsert = Upsert.new connection, :pets
    upsert.row({:name => 'NotJerry'}, :lovability => small)
    Pet.first.lovability.must_equal small
  end
end