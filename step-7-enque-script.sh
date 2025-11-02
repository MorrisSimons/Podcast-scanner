#!/bin/bash

# Generate 10,000 XADD commands and pipe them to redis-cli
for i in $(seq 1 10000); do
    echo "XADD podcast:queue * key dummy-audio-$i.mp3"
done | redis-cli -h 163.172.143.150 -p 6379 --user morris-redis -a 'RMC-gxa1wnw8zwc5uax' --tls --cacert "/Users/morrissimons/Desktop/Podcast scanner/SSL_redis-redis-epic-wing.pem" --pipe
