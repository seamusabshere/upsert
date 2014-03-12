require 'spec_helper'
describe Upsert do
  describe "logger" do
    it "logs where you tell it" do
      begin
        old_logger = Upsert.logger
        io = StringIO.new
        Thread.exclusive do
          Upsert.logger = Logger.new(io)

          Upsert.logger.warn "hello"

          io.rewind
          io.read.chomp.should == 'hello'
        end
      ensure
        Upsert.logger = old_logger
      end
    end

    it "logs queries" do
      begin
        old_logger = Upsert.logger
        io = StringIO.new
        Thread.exclusive do
          Upsert.logger = Logger.new(io)
          
          u = Upsert.new($conn, :pets)
          u.row(:name => 'Jerry')

          io.rewind
          log = io.read.chomp
          case u.connection.class.name
          when /sqlite/i
            log.should =~ /insert or ignore/i
          when /mysql/i
            log.should =~ /call #{Upsert::MergeFunction::NAME_PREFIX}_pets_SEL_name/i
          when /p.*g/i
            log.should =~ /select #{Upsert::MergeFunction::NAME_PREFIX}_pets_SEL_name/i
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
