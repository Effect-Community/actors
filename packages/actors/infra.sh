#!/bin/sh
docker run -d -p 2181:2181 -e ZOO_MY_ID=1 --rm zookeeper:3.7.0
docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pass -e POSTGRES_DB=db --rm postgres:alpine