# Check the liveness of the system by querying the redis liveness hash
# relies on production env. vars. being set.

import os
import redis
from datetime import datetime, timezone, timedelta

def main():
    # Load environment variables
    redis_user = os.environ.get("REDIS_USER")
    redis_password = os.environ.get("REDIS_PASSWORD")
    redis_host = os.environ.get("REDIS_HOSTNAME")
    redis_port = int(os.environ.get("REDIS_PORT", 6379))
    instance_id = os.environ.get("DATABASE_INSTANCE_ID")

    if not all([redis_user, redis_password, redis_host, redis_port, instance_id]):
        raise ValueError("One or more required environment variables are missing.")

    # Construct the hash key
    hash_key = f"{instance_id}:hash:liveness"

    # Create Redis connection (SSL enabled, DB 1)
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        username=redis_user,
        password=redis_password,
        ssl=True,
        db=1
    )

    try:
        # Fetch all key-value pairs from the hash
        data = r.hgetall(hash_key)
        now = datetime.now(timezone.utc)

        # Create dict: system_name -> (diff_rounded, human_timestamp)
        system_diff = {}

        # Decode and print them
        for system_name, timestamp_ms in data.items():
            ts_int = int(timestamp_ms)
            ts_dt = datetime.fromtimestamp(ts_int / 1000, tz=timezone.utc)
            ts_human = ts_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            diff = now - ts_dt 
            diff_rounded = timedelta(seconds=round(diff.total_seconds()))

            system_diff[system_name.decode('utf-8')] = (diff_rounded, ts_human)

        # Sort by system name
        for system_name in sorted(system_diff.keys()):
            diff_rounded, ts_human = system_diff[system_name]

            if diff_rounded.total_seconds() < 15:
                print(f"{system_name}: OK")
            else:
                print(f"{system_name}: {ts_human}, {diff_rounded} ago")

    except redis.RedisError as e:
        print(f"Redis error: {e}")

if __name__ == "__main__":
    main()
