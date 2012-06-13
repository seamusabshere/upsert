require 'helper'
require 'pg'

system %{ dropdb test_upsert }
system %{ createdb test_upsert }

describe "upserting on postgresql" do
  before do
    @opened_connections = []
    @connection = new_connection
    connection.query %{ DROP TABLE IF EXISTS "pets" }
    connection.query %{ CREATE TABLE "pets" ("name" varchar(255) PRIMARY KEY, "gender" varchar(255)) }
  end
  after do
    @opened_connections.each { |c| c.finish }
  end
  def new_connection
    c = PG.connect(:dbname => 'test_upsert')
    @opened_connections << c
    c
  end
  def connection
    @connection
  end
  def select_one(sql)
    res = connection.exec sql
    res.getvalue 0, 0
  end

  def count_sql(table_name, row)
    %{ SELECT COUNT(*) FROM "#{table_name}" WHERE #{row.map { |k, v| v.nil? ? %{ "#{k}" IS NULL } : %{ "#{k}" = '#{v}' }}.join(' AND ')}}
  end

  it_behaves_like :database

end
