import redis
import sys

if len(sys.argv) > 1:
    redis_db = sys.argv[1]
else:
    redis_db = 0

# Connect to the Redis server
redis_host = 'localhost'
redis_port = 6379
redis_user = 'default'
r = redis.StrictRedis(host=redis_host, port=redis_port, db=redis_db, encoding="utf-8", decode_responses=True)

print(f"Dumping Redis database: {redis_db}\n")

keys = r.keys('*')
for key in keys:
    type = r.type(key)
    val = None
    vals = []
    if type == "string":
        val = r.get(key)
    if type == "hash":
        vals = r.hgetall(key)
    if type == "zset":
        vals = r.zrange(key, 0, -1)
    if type == "list":
        vals = r.lrange(key, 0, -1)
    if type == "set":
        vals = r.smembers(key)

    print(f"Key: {key}, Type: {type}");
    if val:
        print(f"  -> {val}")
    elif vals:
        if type == "list" or type == "set" or type == "zset":
            print("  Values:", vals)
        else:
            for v in vals:
                print(f"  -> {v} = {vals[v]}")
    print()