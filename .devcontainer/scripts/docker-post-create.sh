#!/bin/bash

# Create services required for the project
docker volume ls | grep -q "chronoqueueData" || docker volume create chronoqueueData
docker volume ls | grep -q "redisinsightData" || docker volume create redisinsightData

if [ -z "docker container ls -a | grep -q \"redis\"" ]; then
    docker run -d --name redis -e ALLOW_EMPTY_PASSWORD=yes -p 6379:6379 -v chronoqueueData:/data bitnami/redis:latest
else
    docker start redis
fi

if [ -z "docker container ls -a | grep -q \"redisinsight\"" ]; then
    docker run -d --name redisinsight -p 5540:5540 -v redisinsightData:/db redislabs/redisinsight:latest
else
    docker start redisinsight
fi

# Validate that the services are running
docker container ls | grep -q "redis"
docker container ls | grep -q "redisinsight"