from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=1000)
print('set consumer')

consumer.subscribe(['com.udacity.sf.crime.calls'])
print('set subscribe')

for message in consumer:
    print(message.value)