require 'benchmark'
require 'faker'

shared_examples_for 'something that can be speeded up with upserting' do
  describe "speed of upserting" do
    before do
      @fakes = []
      200.times do
        fake = {
          :name => Faker::Name.name,
          :tag_number => rand(1e8),
          :birthday => Time.at(rand * Time.now.to_i),
          :home_address => Faker::Address.street_address
        }
        @fakes << fake
      end
    end
    it "is faster than just creating records with ActiveRecord" do
      # dry run
      @fakes.each do |fake|
        pet = Pet.new
        fake.each do |k, v|
          pet.send "#{k}=", v
        end
        pet.save!
      end
      Pet.delete_all
      ar_time = Benchmark.realtime do
        @fakes.each do |fake|
          pet = Pet.new
          fake.each do |k, v|
            pet.send "#{k}=", v
          end
          pet.save!
        end
      end
      Pet.delete_all
      upsert_time = Benchmark.realtime do
        upsert = Upsert.new connection, :pets
        upsert.multi do |xxx|
          @fakes.each do |fake|
            xxx.row(fake.slice(:name), fake.except(:name))
          end
        end
      end
      $stderr.puts "   Upsert was #{((ar_time - upsert_time) / ar_time * 100).round}% faster than ActiveRecord new/save"
      upsert_time.must_be :<, ar_time
    end
  end
end
