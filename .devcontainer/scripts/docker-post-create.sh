#!/bin/bash

# Create services required for the project
docker volume ls | grep -q "chronoqueueData" || docker volume create chronoqueueData
docker volume ls | grep -q "redisinsightData" || docker volume create redisinsightData

if [ -z "docker container ls -a | grep -q redis" ]; then
    echo "Starting redis"
    docker start redis
else
    echo "Pulling and running Redis"
    docker pull bitnami/redis:latest
    docker run -d --name redis -e ALLOW_EMPTY_PASSWORD=yes -p 6379:6379 -v chronoqData:/data bitnami/redis:latest
fi

if [ -z "docker container ls -a | grep -q redisinsight" ]; then
    echo "Starting RedisInsight"
    docker start redisinsight
else
    echo "Pulling and running RedisInsight"
    docker pull redislabs/redisinsight:latest
    docker run -d --name redisinsight -p 5540:5540 -v redisinsightData:/db redislabs/redisinsight:latest
fi

# Validate that the services are running
docker container ls | grep -q "redis"
docker container ls | grep -q "redisinsight"