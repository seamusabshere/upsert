require 'helper'
require 'mysql2'

system %{ mysql -u root -ppassword -e "DROP DATABASE IF EXISTS test_upsert; CREATE DATABASE test_upsert CHARSET utf8" }

describe "upserting on mysql2" do
  before do
    @opened_connections = []
    @connection = new_connection
    connection.query %{ DROP TABLE IF EXISTS "pets" }
    connection.query %{ CREATE TABLE "pets" ("name" varchar(255) PRIMARY KEY, "gender" varchar(255)) }
  end
  after do
    @opened_connections.each { |c| c.close }
  end
  def new_connection
    c = Mysql2::Client.new(:username => 'root', :password => 'password', :database => 'test_upsert')
    @opened_connections << c
    c
  end
  def connection
    @connection
  end
  def select_one(sql)
    connection.query(sql, :as => :array).first.first
  end

  def count_sql(table_name, row)
    %{ SELECT COUNT(*) FROM "#{table_name}" WHERE #{row.map { |k, v| v.nil? ? %{ `#{k}` IS NULL } : %{ `#{k}` = '#{v}' }}.join(' AND ')}}
  end

  it_behaves_like :database

end
