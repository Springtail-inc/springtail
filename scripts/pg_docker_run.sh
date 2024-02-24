#!/bin/bash
docker stop pg14
docker rm pg14
docker run --name pg14 -d -p 5432:5432 -v "$PWD/docker-postgres.conf":/etc/postgresql/postgresql.conf -e POSTGRES_PASSWORD=springtail postgres:14.5 -c 'config_file=/etc/postgresql/postgresql.conf'
echo "Created postgres running on port 5432 with name pg14"
docker ps -a | grep postgres:14.5