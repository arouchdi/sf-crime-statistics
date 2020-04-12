import asyncio

from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "sf-crimes-consumer"})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=0.1)
        for message in messages:
            if message is None:
                print("message is None")
            elif message.error() is not None:
                print(f"error: {message.error()}")
            else:
                print(f"{message.value()}")

        await asyncio.sleep(0.01)


def main():
    try:
        asyncio.run(consume("sf.crimes"))
    except KeyboardInterrupt as e:
        print("Shutting down")


if __name__ == "__main__":
    main()