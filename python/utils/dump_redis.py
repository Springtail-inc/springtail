import redis
import sys
import os


if len(sys.argv) > 1:
    redis_db = sys.argv[1]
else:
    redis_db = os.environ.get('REDIS_CONFIG_DATABASE', 0)

# Connect to the Redis server
redis_host = os.environ.get('REDIS_HOST', 'localhost')
redis_port = int(os.environ.get('REDIS_PORT', 6379))
redis_user = os.environ.get('REDIS_USER', 'default')
redis_password = os.environ.get('REDIS_PASSWORD', None)
r = redis.StrictRedis(host=redis_host, port=redis_port,
                      username=redis_user,
                      db=redis_db, password=redis_password,
                      encoding="utf-8", decode_responses=True)

instance_id = os.environ.get('DATABASE_INSTANCE_ID', '1234')

print(f"Dumping Redis database={redis_db}, instance_id={instance_id}\n")

keys = r.keys(instance_id + ':*')
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