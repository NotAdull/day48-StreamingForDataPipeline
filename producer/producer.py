#!/usr/bin/env python
# coding: utf-8

# In[4]:


get_ipython().system('pip install confluent-kafka')


# In[ ]:


import json
import time
import random
from datetime import datetime, timezone, timedelta
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'kafka:9092'})

def delivery_report(err, msg):
    if err:
        print(f'Gagal: {err}')
    else:
        print(f'Terkirim → topic: {msg.topic()} | partition: {msg.partition()}')

def kirim(data):
    producer.produce(
        topic='transactions',
        value=json.dumps(data).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)

# Data Normal
def buat_event_normal():
    return {
        "user_id": f"U{random.randint(1000, 9999)}",
        "amount": random.randint(10000, 5000000),
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": random.choice(["mobile", "web", "pos"])
    }

# Data INVALID 
invalid_events = [
    # 1. Amount negatif
    {
        "user_id": "U0001",
        "amount": -500,
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": "mobile"
    },
    # 2. Source tidak dikenal
    {
        "user_id": "U0002",
        "amount": 50000,
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": "unknown_source"
    },
    # 3. Timestamp tidak valid
    {
        "user_id": "U0003",
        "amount": 30000,
        "timestamp": "BUKAN-TANGGAL",
        "source": "web"
    },
    # 4. Amount terlalu besar
    {
        "user_id": "U0004",
        "amount": 99999999,
        "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "source": "pos"
    },
]

# Data LATE EVENTS 
late_events = [
    {
        "user_id": "U9001",
        "amount": 75000,
        "timestamp": "2020-01-01T00:00:00Z",
        "source": "mobile"
    },
    {
        "user_id": "U9002",
        "amount": 120000,
        "timestamp": "2019-06-15T10:30:00Z",
        "source": "web"
    },
    {
        "user_id": "U9003",
        "amount": 200000,
        "timestamp": "2018-03-20T08:00:00Z",
        "source": "pos"
    },
]

print("Producer mulai berjalan...")

print("Mengirim data invalid...")
for event in invalid_events:
    kirim(event)
    time.sleep(1)

print("Mengirim late events...")
for event in late_events:
    kirim(event)
    time.sleep(1)

print("Mengirim data normal...")
while True:
    event = buat_event_normal()
    kirim(event)
    print(f"Normal → {event}")
    time.sleep(random.uniform(1, 2))  


# In[ ]:




