#!/usr/bin/bash
db_path=./src/.temp/database.db
echo "opening db on ${db_path}"
sqlitebrowser $db_path
