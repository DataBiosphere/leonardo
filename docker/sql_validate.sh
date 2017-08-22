#!/bin/bash

SERVICE=$1

# validate mysql
echo "sleeping for 60 seconds during mysql boot..."
sleep 60
mysql -uroot -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -u${SERVICE}-test -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -u${SERVICE}-test -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()" testdb