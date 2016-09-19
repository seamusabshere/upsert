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
        Upsert.new($conn_factory.new_connection, :pets, use_native_upsert: false).row :name => 'hello'

        # clear, create (#2)
        Upsert.clear_database_functions($conn_factory.new_connection)
        Upsert.new($conn_factory.new_connection, :pets, use_native_upsert: false).row :name => 'hello'

        io.rewind
        hits = io.read.split("\n").grep(/Creating or replacing/)
        hits.length.should == 2
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
        Upsert.new(connection, :pets, use_native_upsert: false).row :name => 'hello'

        # clear, create (#2)
        Upsert.clear_database_functions(connection)
        Upsert.new(connection, :pets, use_native_upsert: false).row :name => 'hello'

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
        Upsert.batch($conn_factory.new_connection, :pets, use_native_upsert: false) do |upsert|
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
        Upsert.new($conn_factory.new_connection, :pets, :assume_function_exists => true, use_native_upsert: false).row :name => 'hello'

        # just works
        Upsert.new($conn_factory.new_connection, :pets, :assume_function_exists => true, use_native_upsert: false).row :name => 'hello'

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
