#!/bin/bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# run only if data dir is empty. See "Initialization scripts" section on https://hub.docker.com/_/postgres for more details.
# script based on https://github.com/xtimon/postgres

# USAGE
# docker run --name some-postgres -p 5432:5432 -e POSTGRES_USERS="user1:user1pass|user2:user2pass|user3:user3password" -e POSTGRES_DATABASES="database1:user1|database2:user2|database3:user3"

set -e

psql=( psql -v ON_ERROR_STOP=1 )

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
