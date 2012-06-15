shared_examples_for 'is just as correct as other ways' do
  describe :correctness do
    describe 'compared to native ActiveRecord' do
      it "is faster than new/set/save" do
        assert_same_result lotsa_records do |records|
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
        assert_same_result lotsa_records do |records|
          dynamic_method = nil
          records.each do |selector, document|
            dynamic_method ||= "find_or_create_by_#{selector.keys.join('_or_')}"
            pet = Pet.send(dynamic_method, *selector.values)
            pet.update_attributes document, :without_protection => true
          end
        end
      end
      it "is faster than create + rescue/find/update" do
        assert_same_result lotsa_records do |records|
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
