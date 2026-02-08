
import redis

if __name__ == "__main__":
    # Connect to local Redis instance
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    p = client.pubsub()

    channel_name = "friend::1"
    p.subscribe(channel_name)

    print(f"Subscribed to channel: {channel_name}")

    # Continuous loop to listen for new data
    for message in p.listen():
        # Redis sends a 'subscribe' confirmation message first; we skip it
        if message['type'] == 'message':
            print(f"Received: {message['data']}")
