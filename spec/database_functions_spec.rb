require 'spec_helper'
require 'stringio'
describe Upsert do
  if ENV['ADAPTER'] == 'postgresql'
    describe 'PostgreSQL database functions' do
      it "re-uses merge functions across connections" do
        begin
          io = StringIO.new
          old_logger = Upsert.logger
          Upsert.logger = Logger.new io, Logger::INFO

          # clear
          Upsert.clear_database_functions(PGconn.new(:dbname => 'upsert_test'))
          
          # create
          Upsert.new(PGconn.new(:dbname => 'upsert_test'), :pets).row :name => 'hello'

          # clear
          Upsert.clear_database_functions(PGconn.new(:dbname => 'upsert_test'))

          # create (#2)
          Upsert.new(PGconn.new(:dbname => 'upsert_test'), :pets).row :name => 'hello'

          # no create!
          Upsert.new(PGconn.new(:dbname => 'upsert_test'), :pets).row :name => 'hello'
          
          io.rewind
          hits = io.read.split("\n").grep(/Creating or replacing/)
          hits.length.should == 2
        ensure
          Upsert.logger = old_logger
        end
      end
    end
  end
end
