require 'spec_helper'
describe Upsert do
  MUTEX_FOR_PERFORM = Mutex.new
  describe "logger" do
    it "logs where you tell it" do
      begin
        old_logger = Upsert.logger
        io = StringIO.new
        MUTEX_FOR_PERFORM.synchronize do
          Upsert.logger = Logger.new(io)

          Upsert.logger.warn "hello"

          io.rewind
          expect(io.read.chomp).to eq('hello')
        end
      ensure
        Upsert.logger = old_logger
      end
    end

    it "logs queries" do
      begin
        old_logger = Upsert.logger
        io = StringIO.new
        MUTEX_FOR_PERFORM.synchronize do
          Upsert.logger = Logger.new(io)
          
          u = Upsert.new($conn, :pets)
          u.row(:name => 'Jerry')

          io.rewind
          log = io.read.chomp
          case u.connection.class.name
          when /sqlite/i
            expect(log).to match(/insert or ignore/i)
          when /mysql/i
            expect(log).to match(/call #{Upsert::MergeFunction::NAME_PREFIX}_pets_SEL_name/i)
          when /p.*g/i
            # [54ae2eea857] Possibly much more useful debug output
            expect(log).to match(/selector:/i)
          else
            raise "not sure"
          end
        end
      ensure
        Upsert.logger = old_logger
      end
    end
  end
end
