require 'spec_helper'
describe Upsert do
  describe "logger" do
    it "logs to stderr by default" do
      begin
        old_stderr = $stderr
        $stderr = StringIO.new
        Upsert.logger.warn "hello"
        $stderr.rewind
        $stderr.read.chomp.should == 'hello'
      ensure
        $stderr = old_stderr
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
        io.read.chomp.should =~ /INSERT OR IGNORE.*you/i
      ensure
        Upsert.logger = old_logger
      end
    end

  end
end
