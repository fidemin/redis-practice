import redis
import threading
import sys

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def listen_for_messages(channel, username):
    pubsub = r.pubsub()
    pubsub.subscribe(channel)

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data']
            # Don't print our own messages back to us
            if not data.startswith(f"[{username}]"):
                print(f"\n{data}\n> ", end="")


def start_chat():
    username = input("Enter your username: ")
    room = input("Enter chat room name: ")

    # 2. Start the background listener thread
    listener = threading.Thread(
        target=listen_for_messages,
        args=(room, username),
        daemon=True
    )
    listener.start()

    print(f"--- Joined {room} as {username}. Type your message and hit Enter ---")

    # 3. Main loop for sending messages
    try:
        while True:
            text = input("> ")
            if text.lower() in ['exit', 'quit']:
                break

            # Format and publish the message
            full_msg = f"[{username}]: {text}"
            r.publish(room, full_msg)
    except KeyboardInterrupt:
        pass

    print("\nLeft the chat.")


if __name__ == "__main__":
    start_chat()
