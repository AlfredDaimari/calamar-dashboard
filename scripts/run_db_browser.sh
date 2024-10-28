#!/usr/bin/bash
db_path=./src/calamar_backend/.temp/database.db
echo "opening db on ${db_path}"
sqlitebrowser $db_path
