from kafka import KafkaConsumer

consumer = KafkaConsumer(
    bootstrap_servers="localhost:9092",
    api_version=(4, 0, 0)
)

print("Connected")
print("Topics:", consumer.topics())

consumer.close()