require 'spec_helper'
require 'stringio'
describe Upsert do
  def fresh_connection
    case ENV['ADAPTER']
    when 'postgresql'
      PGconn.new $conn_config
    when 'mysql2'
      Mysql2::Client.new $conn_config
    end
  end
  describe 'database functions' do
    it "re-uses merge functions across connections" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO

        # clear
        Upsert.clear_database_functions(fresh_connection)
        
        # create
        Upsert.new(fresh_connection, :pets).row :name => 'hello'

        # clear
        Upsert.clear_database_functions(fresh_connection)

        # create (#2)
        Upsert.new(fresh_connection, :pets).row :name => 'hello'

        # no create!
        Upsert.new(fresh_connection, :pets).row :name => 'hello'
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        hits.length.should == 2
      ensure
        Upsert.logger = old_logger
      end
    end
  end
end if %w{ postgresql mysql2 }.include?(ENV['ADAPTER'])
