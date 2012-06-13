shared_examples_for :database do
  describe :row do
    it "works for a single row" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.row({:name => 'Jerry'}, {:gender => 'male'})
      end
    end
    it "works for a single row with nulls" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Inky', :gender => nil}]) do
        upsert.row({:name => 'Inky'}, {:gender => nil})
      end
    end
    # it "works for a single row upserted many times" do
    #   assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
    #     ts = (0..5).map do
    #       Thread.new do
    #         upsert = Upsert.new new_connection, :pets
    #         upsert.row({:name => 'Jerry'}, {:gender => 'male'})
    #       end
    #     end
    #     ts.each { |t| t.join }
    #   end
    # end
  end
  describe :multi do
    it "works for multiple rows" do
      upsert = Upsert.new connection, :pets
      assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
        upsert.multi do
          row({:name => 'Jerry'}, :gender => 'male')
        end
      end
    end
    # it "works for multiple rows upserted many times" do
    #   assert_creates(Pet, [{:name => 'Jerry', :gender => 'male'}]) do
    #     ts = (0..5).map do
    #       Thread.new do
    #         upsert = Upsert.new new_connection, :pets
    #         upsert.multi do
    #           row({:name => 'Jerry'}, :gender => 'male')
    #           row({:name => 'Jerry'}, :gender => 'male')
    #         end
    #       end
    #     end
    #     ts.each { |t| t.join }
    #   end
    # end
  end
end
