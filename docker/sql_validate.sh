#!/bin/bash

SERVICE=$1

# validate mysql
echo "Checking every 3s for mysql to be ready"
while ! mysqladmin ping --host=mysql --port=3306 --silent; do
  sleep 3
done

mysql -uroot -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -u${SERVICE}-test -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()"
mysql -u${SERVICE}-test -p${SERVICE}-test --host=mysql --port=3306 -e "SELECT VERSION();SELECT NOW()" leotestdb