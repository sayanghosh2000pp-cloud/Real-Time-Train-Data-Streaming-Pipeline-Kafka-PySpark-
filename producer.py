
import json, random, time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

trains = ['12623','12624','12345','02012','11015']
stations = ['NDLS','BCT','HWH','MAS','SBC','PUNE']

while True:
    event = {
        'train_no': random.choice(trains),
        'timestamp': int(time.time()),
        'station': random.choice(stations),
        'status': random.choice(['on_time','delayed']),
        'delay_min': random.randint(0,120)
    }
    producer.send('train_events', event)
    print('sent:', event)
    time.sleep(1)
