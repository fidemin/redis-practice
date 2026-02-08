import redis

if __name__ == "__main__":
    client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    channel_name = "friend::1"

    while True:
        message = input(">>")
        if message.lower() in ['exit', 'quit']:
            break
        client.publish(channel_name, message)

        print(f"Sent: {message}")
    print("Exiting publisher.")
