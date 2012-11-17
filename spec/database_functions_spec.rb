require 'spec_helper'
require 'stringio'
describe Upsert do
  describe 'database functions' do
    it "re-uses merge functions across connections" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO

        # clear
        Upsert.clear_database_functions($conn_factory.new_connection)
        
        # create
        Upsert.new($conn_factory.new_connection, :pets).row :name => 'hello'

        # clear
        Upsert.clear_database_functions($conn_factory.new_connection)

        # create (#2)
        Upsert.new($conn_factory.new_connection, :pets).row :name => 'hello'

        # no create!
        Upsert.new($conn_factory.new_connection, :pets).row :name => 'hello'
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        hits.length.should == 2
      ensure
        Upsert.logger = old_logger
      end
    end
  end
end if %w{ postgresql mysql }.include?(ENV['DB'])
