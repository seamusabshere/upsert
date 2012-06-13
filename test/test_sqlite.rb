require 'helper'
require 'sqlite3'

describe "upserting on sqlite" do
  before do
    @connection = SQLite3::Database.new(':memory:')
    connection.execute %{ CREATE TABLE "pets" ("name" varchar(255), "gender" varchar(255)) }
    connection.execute %{ CREATE UNIQUE INDEX "index_#{'pets'}_on_name" ON "pets" ("name") }
  end
  def new_connection
    @connection
  end
  def connection
    @connection
  end
  def select_one(sql)
    connection.execute(sql).first.first
  end

  def count_sql(table_name, row)
    %{ SELECT COUNT(*) FROM "#{table_name}" WHERE #{row.map { |k, v| v.nil? ? %{ "#{k}" IS NULL } : %{ "#{k}" = "#{v}" }}.join(' AND ')}}
  end

  it_behaves_like :database
end
