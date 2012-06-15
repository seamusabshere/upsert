shared_examples_for 'something that can be speeded up with upserting' do
  describe "speed" do
    before do
      @fakes = []
      names = []
      50.times do
        names << Faker::Name.name
      end
      200.times do
        selector = ActiveSupport::OrderedHash.new
        selector[:name] = names.sample(1)[0]
        document = {
          :tag_number => rand(1e8),
          :birthday => Time.at(rand * Time.now.to_i),
          :home_address => Faker::Address.street_address
        }
        @fakes << [selector, document]
      end
    end

    describe 'compared to native ActiveRecord' do
      it "is faster than new/set/save" do
        assert_faster_than 'find + new/set/save', @fakes do |records|
          records.each do |selector, document|
            if pet = Pet.where(selector).first
              pet.update_attributes document, :without_protection => true
            else
              pet = Pet.new
              selector.each do |k, v|
                pet.send "#{k}=", v
              end
              document.each do |k, v|
                pet.send "#{k}=", v
              end
              pet.save!
            end
          end
        end
      end
      it "is faster than find_or_create + update_attributes" do
        assert_faster_than 'find_or_create + update_attributes', @fakes do |records|
          dynamic_method = nil
          records.each do |selector, document|
            dynamic_method ||= "find_or_create_by_#{selector.keys.join('_or_')}"
            pet = Pet.send(dynamic_method, *selector.values)
            pet.update_attributes document, :without_protection => true
          end
        end
      end
      it "is faster than create + rescue/find/update" do
        assert_faster_than 'create + rescue/find/update', @fakes do |records|
          dynamic_method = nil
          records.each do |selector, document|
            dynamic_method ||= "find_or_create_by_#{selector.keys.join('_or_')}"
            begin
              Pet.create selector.merge(document), :without_protection => true
            rescue
              pet = Pet.send(dynamic_method, *selector.values)
              pet.update_attributes document, :without_protection => true
            end
          end
        end
      end
    end

    # describe 'compared to activerecord-import' do
    #   assert_faster_than 'how it uses ON DUPLICATE KEY' do
    #     columns = []
    #     values = []
    #     selector.each do |k, v|
    #       columns << k
    #       values << v
    #     end
    #     document.each do |k, v|
    #       columns << k
    #       values << v
    #     end
    #     on_duplicate_key_update = if insert_only
    #       selector.keys
    #     else
    #       selector.keys + document.keys
    #     end
    #     attempt = 0
    #     # until record = first(:conditions => selector)
    #     begin
    #       if attempt > 0
    #         ::Kernel.srand
    #         wait_time = ::Kernel.rand*(2**attempt)
    #         $stderr.puts "Upsert #{name} with #{selector}: attempt #{attempt}, waiting #{wait_time}..."
    #         ::Kernel.sleep wait_time
    #       end
    #       import columns, [values], :timestamps => false, :on_duplicate_key_update => on_duplicate_key_update

    #   end
    # end
  end
end
