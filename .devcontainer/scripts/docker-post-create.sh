#!/bin/bash

# Create services required for the project
docker volume ls | grep -q "chronoqueueData" || docker volume create chronoqueueData
docker volume ls | grep -q "redisinsightData" || docker volume create redisinsightData
docker ps | grep -q "redis" || docker run -d --name redis -e ALLOW_EMPTY_PASSWORD=yes -p 6379:6379 -v chronoqueueData:/data bitnami/redis:latest
docker ps | grep -q "redisinsight" || docker run -d --name redisinsight -p 5540:5540 -v redisinsightData:/db redislabs/redisinsight:latest