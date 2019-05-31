require "remote_table"

a = RemoteTable.new(
  url: "http://www.postgresql.org/docs/9.1/static/sql-keywords-appendix.html",
  row_css: "table.CALSTABLE tbody tr",
  column_css: "td",
  headers: %w[key_word]
)

a.each do |row|
  puts row["key_word"]
end
