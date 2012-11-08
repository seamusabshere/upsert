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
      require 'sqlite3'
      db = SQLite3::Database.open(':memory:')
      db.execute_batch "CREATE TABLE cats (name CHARACTER VARYING(255))"
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::DEBUG
        u = Upsert.new(db, :cats)
        u.row :name => 'you'
        io.rewind
        io.read.chomp.should =~ /INSERT OR IGNORE.*you/mi
      ensure
        Upsert.logger = old_logger
      end
    end

  end
end
