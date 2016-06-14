require 'spec_helper'
require 'stringio'
describe Upsert do
  describe 'database functions' do
    it "does not re-use merge functions across connections" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO

        # clear, create (#1)
        Upsert.clear_database_functions($conn_factory.new_connection)
        Upsert.new($conn_factory.new_connection, :pets).row :name => 'hello'

        # clear, create (#2)
        Upsert.clear_database_functions($conn_factory.new_connection)
        Upsert.new($conn_factory.new_connection, :pets).row :name => 'hello'
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        expect(hits.length).to eq(2)
      ensure
        Upsert.logger = old_logger
      end
    end
    
    it "does not re-use merge functions even when on the same connection" do
      begin
        io = StringIO.new
        old_logger = Upsert.logger
        Upsert.logger = Logger.new io, Logger::INFO
        
        connection = $conn_factory.new_connection

        # clear, create (#1)
        Upsert.clear_database_functions(connection)
        Upsert.new(connection, :pets).row :name => 'hello'

        # clear, create (#2)
        Upsert.clear_database_functions(connection)
        Upsert.new(connection, :pets).row :name => 'hello'
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        expect(hits.length).to eq(2)
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
        Upsert.batch($conn_factory.new_connection, :pets) do |upsert|
          upsert.row :name => 'hello'
          upsert.row :name => 'world'
        end
        
        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        expect(hits.length).to eq(1)
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
        expect(lines.grep(/went missing/).length).to eq(1)
        expect(lines.grep(/Creating or replacing/).length).to eq(1)
      ensure
        Upsert.logger = old_logger
      end
    end

  end
end if %w{ postgresql mysql }.include?(ENV['DB'])
