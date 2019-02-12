main:
	sh runParser.sh
	sqlite3 ebay_data.db < create.sql
	sqlite3 ebay_data.db < load.txt