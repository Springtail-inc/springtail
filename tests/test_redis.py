import redis
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_redis_operations():
    logging.info("Connecting to Redis")
    r = redis.StrictRedis(host="localhost", port=6379, db=0)

    # Set a value
    logging.info("Setting key 'test_key' in Redis")
    r.set('test_key', 'test_value')

    # Get the value
    logging.info("Retrieving key 'test_key' from Redis")
    value = r.get('test_key').decode('utf-8')
    logging.info(f"Retrieved value: {value}")
    
    # Delete the key
    logging.info("Deleting key 'test_key' from Redis")
    r.delete('test_key')

    # Check if the key was deleted
    deleted_value = r.get('test_key')
    logging.info(f"Deleted key, value after delete: {deleted_value}")
