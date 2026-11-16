#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./gradlew bootJar -q
exec java -jar build/libs/lab3-nosql-1.0.0.jar \
    --logging.level.com.university.booking=INFO \
    --logging.level.org.springframework.data.redis=INFO \
    --logging.level.org.springframework.data.mongodb.core.MongoTemplate=INFO "$@"
