import sys, csv
import time
import json
from kafka import KafkaProducer
import datetime
import yaml

with open(r'config.yaml') as file:
    cfg = yaml.load(file, Loader=yaml.FullLoader)

    KAFKA_SERVER = cfg['global']['bootstrap_server']
    DATA_FILE = cfg['global']['data_file']
    KAFKA_TOPIC = cfg['global']['topic']
    LINGER_MS = cfg['producer']['linger_ms']
    DELAY_SEC = cfg['global']['delay_sec']
    BATCH_SIZE = cfg['producer']['batch_size']
    RECORD_CNT = cfg['global']['record_cnt']
    RUN_FLAG = cfg['global']['running']

def error(exception):
    print(exception)

def kafka_python_producer_async(_producer, _msg, _topic):
    _producer.send(topic = _topic, value = _msg).add_errback(error)
    _producer.flush()
    
cnt = 0
cnt_window = 0
producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, linger_ms = LINGER_MS)
exp_time = datetime.datetime.now().timestamp()

WINDOW_INTERVAL = 20

while True:
    data = csv.reader(open(DATA_FILE), delimiter=",")
    colnames = ['actor_login', 'actor_id', 'comment_id', 'comment', 'repo', 'language', 
                'author_login', 'author_id', 'pr_id', 'c_id', 'commit_date', 
                'topic', 'delay_sec', 'batch_size', 'linger_ms', 'timestamp']

    next(data, None)  # skip the headers
  
    for r in data:
        
        time_diff = datetime.datetime.now().timestamp() - exp_time
        
        
        if time_diff>WINDOW_INTERVAL:
            print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "total events sent:", cnt, "last", WINDOW_INTERVAL, "seconds:", cnt_window, "running:", RUN_FLAG)
            cnt_window = 0
            exp_time = datetime.datetime.now().timestamp()
            
            with open(r'config.yaml') as file:
                cfg = yaml.load(file, Loader=yaml.FullLoader)

                KAFKA_SERVER = cfg['global']['bootstrap_server']
                DATA_FILE = cfg['global']['data_file']
                KAFKA_TOPIC = cfg['global']['topic']
                LINGER_MS = cfg['producer']['linger_ms']
                DELAY_SEC = cfg['global']['delay_sec']
                BATCH_SIZE = cfg['producer']['batch_size']
                RECORD_CNT = cfg['global']['record_cnt']
                RUN_FLAG = cfg['global']['running']
                
            producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, linger_ms = LINGER_MS)
            
        
        msg = r
        msg.append(KAFKA_TOPIC)
        msg.append(DELAY_SEC)
        msg.append(BATCH_SIZE)
        msg.append(LINGER_MS)    
        msg.append(datetime.datetime.now().timestamp())    
        msg_flat = ';'.join(str(m) for m in msg)
        
        if RUN_FLAG == 1:
            kafka_python_producer_async(producer, msg_flat.encode(), KAFKA_TOPIC)
            cnt = cnt + 1
            cnt_window = cnt_window + 1
        
        time.sleep(DELAY_SEC)
        
        #print(msg_flat)

        if (cnt > RECORD_CNT) and (RECORD_CNT > 0):
            print("Maximum number of events reached:", RECORD_CNT)
            break
        
    if (cnt > RECORD_CNT) and (RECORD_CNT > 0):
        print("Maximum number of events reached:", RECORD_CNT)
        break
    
    