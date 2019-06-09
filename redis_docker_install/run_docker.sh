docker run --name=redis-practice -d --restart=always -p 9005:6379 -v $(pwd)/redis.conf:/usr/local/etc/redis/redis.conf redis /usr/local/etc/redis/redis.conf
