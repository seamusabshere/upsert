require 'benchmark'
require 'faker'

shared_examples_for 'something that can be speeded up with upserting' do
  describe "speed of upserting" do
    before do
      $fakes = []
      200.times do
        fake = {
          :name => Faker::Name.name,
          :tag_number => rand(1e11),
          :birthday => Time.at(rand * Time.now.to_i),
          :home_address => Faker::Address.street_address
        }
        $fakes << fake
      end
    end
    it "is faster than just creating records with ActiveRecord" do
      # dry run
      # $fakes.each do |fake|
      #   pet = Pet.new
      #   fake.each do |k, v|
      #     pet.send "#{k}=", v
      #   end
      #   pet.save!
      # end
      # Pet.delete_all
      # ar_time = Benchmark.realtime do
      #   $fakes.each do |fake|
      #     pet = Pet.new
      #     fake.each do |k, v|
      #       pet.send "#{k}=", v
      #     end
      #     pet.save!
      #   end
      # end
      upsert_time = Benchmark.realtime do
        upsert = Upsert.new connection, :pets
        # FIXME don't use instance_eval for multi since seems like this is a common pattern...
        upsert.multi do
          $fakes.each do |fake|
            row(fake.slice(:name), fake.except(:name))
          end
        end
        upsert.cleanup
      end
    end
  end
end
