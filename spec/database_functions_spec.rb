require 'spec_helper'
require 'stringio'
describe Upsert do
  describe 'database functions' do

    it "does not re-use merge functions across connections" do
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
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        hits.length.should == 2
      ensure
        Upsert.logger = old_logger
      end
    end
    
    it "re-uses merge functions within batch" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO

        # clear
        Upsert.clear_database_functions($conn_factory.new_connection)
        
        # create
        Upsert.batch(:pets, $conn_factory.new_connection) do |upsert|
          upsert.row :name => 'hello'
          upsert.row :name => 'world'
        end
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        hits.length.should == 1
      ensure
        Upsert.logger = old_logger
      end
    end

    it "assumes function exists if told to" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO

        # clear
        Upsert.clear_database_functions($conn_factory.new_connection)
        
        # tries, "went missing", creates
        Upsert.new($conn_factory.new_connection, :pets, :assume_function_exists => true).row :name => 'hello'

        # just works
        Upsert.new($conn_factory.new_connection, :pets, :assume_function_exists => true).row :name => 'hello'

        io.rewind
        lines = io.read.split("\n")
        lines.grep(/went missing/).length.should == 1
        lines.grep(/Creating or replacing/).length.should == 1
      ensure
        Upsert.logger = old_logger
      end
    end

  end
end if %w{ postgresql mysql }.include?(ENV['DB'])
