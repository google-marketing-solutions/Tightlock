#!/bin/bash

# run only if data dir is empty. See "Initialization scripts" section on https://hub.docker.com/_/postgres for more details.
# script based on https://github.com/xtimon/postgres

# USAGE
# docker run --name some-postgres -p 5432:5432 -e POSTGRES_USERS="user1:user1pass|user2:user2pass|user3:user3password" -e POSTGRES_DATABASES="database1:user1|database2:user2|database3:user3"

set -e

if [ "$POSTGRES_USERS" ]; then
	USERS_ARR=$(echo $POSTGRES_USERS | tr "|" "\n")
	for USER in $USERS_ARR
	do
		USER_NAME=`echo $USER | cut -d: -f1`
		USER_PASSWORD=`echo $USER | cut -d: -f2`
		if [ "$USER_NAME" = 'postgres' ]; then
			op='ALTER'
		else
			op='CREATE'
		fi
		"${psql[@]}" --username postgres <<-EOSQL
			$op USER "$USER_NAME" WITH SUPERUSER PASSWORD '$USER_PASSWORD' ;
		EOSQL
	done
fi

if [ "$POSTGRES_DATABASES" ]; then
	DATABASES_ARR=$(echo $POSTGRES_DATABASES | tr "|" "\n")
	for DATABASE in $DATABASES_ARR
	do
		DATABASE_NAME=`echo $DATABASE | cut -d: -f1`
  	DATABASE_OWNERS=`echo $DATABASE | cut -d: -f2`
  	DATABASE_OWNERS_ARR=$(echo $DATABASE_OWNERS | tr "," "\n")
		if [ "$DATABASE_NAME" != 'postgres' ]; then
			"${psql[@]}" --username postgres <<-EOSQL
				CREATE DATABASE "$DATABASE_NAME" ;
			EOSQL
			echo
		fi
		for USER in $DATABASE_OWNERS_ARR
		do
			"${psql[@]}" --username postgres <<-EOSQL
				GRANT ALL PRIVILEGES ON DATABASE "$DATABASE_NAME" TO "$USER" ;
			EOSQL
		done
	done
fi