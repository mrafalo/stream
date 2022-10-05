import sys, csv, os
import time
import json
from kafka import KafkaProducer
import datetime
import yaml
import logging

def accurate_delay(delay):
    _ = time.perf_counter() + delay/1000
    while time.perf_counter() < _:
        pass
    
with open(r'config.yaml') as file:
    cfg = yaml.load(file, Loader=yaml.FullLoader)

    KAFKA_SERVER = cfg['global']['bootstrap_server']
    DATA_FILE = cfg['global']['data_file']
    KAFKA_TOPICS = cfg['global']['topic']
    LINGER_MS = cfg['producer']['linger_ms']
    DELAY_SWAP = cfg['global']['delay_swap']
    RECORDS_PER_MINUTE = cfg['global']['records_per_minute']
    BATCH_SIZE = cfg['producer']['batch_size']
    RECORD_CNT = cfg['global']['record_cnt']
    RUN_FLAG = cfg['global']['running']
    LOG_LEVEL = cfg['global']['log_level']

def error(exception):
    print(exception)

def kafka_python_producer_send(_producer, _msg, _topic):
    _producer.send(topic = _topic, value = _msg)
    #_producer.flush()
    
cnt = 0
cnt_window = 0
delay_idx = 0

logger = logging.getLogger('spark_stream')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s(%(name)s) %(levelname)s: %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, linger_ms = LINGER_MS)
exp_time = datetime.datetime.now().timestamp()
exp_time60 = datetime.datetime.now().timestamp()
swap_time = datetime.datetime.now().timestamp()

LOG_INTERVAL = 20
records_per_second = RECORDS_PER_MINUTE[delay_idx] / 60
time_list60 = []

while True:
    data = csv.reader(open(DATA_FILE), delimiter = ",", skipinitialspace = True)

    next(data, None)  # skip the headers
  
    for r in data:
        
        time_diff = datetime.datetime.now().timestamp() - exp_time
        time_diff60 = datetime.datetime.now().timestamp() - exp_time60
        swap_diff = datetime.datetime.now().timestamp() - swap_time
        
        if swap_diff > DELAY_SWAP:
            delay_idx = (delay_idx + 1) % len(RECORDS_PER_MINUTE)
            records_per_second = RECORDS_PER_MINUTE[delay_idx] / 60
            swap_time = datetime.datetime.now().timestamp()
            
        if time_diff > LOG_INTERVAL:
            cnt_last60 = len(list(filter(lambda c: c >= datetime.datetime.now().timestamp() - 60, time_list60)))
            cnt_window = len(list(filter(lambda c: c >= datetime.datetime.now().timestamp() - LOG_INTERVAL, time_list60)))
            
            log_msg = "total: " + str(cnt) + " last 60sec: " + str(cnt_last60)\
            + " last " +  str(LOG_INTERVAL) + "sec " + str(cnt_window)\
            + " def speed: " + str(int(records_per_second)) + "/sec"\
            + " true speed: " + str(int(cnt_last60//60)) + "/sec"
            logger.info(log_msg)
           
            exp_time = datetime.datetime.now().timestamp()
            
            with open(r'config.yaml') as file:
                cfg = yaml.load(file, Loader=yaml.FullLoader)

                KAFKA_SERVER = cfg['global']['bootstrap_server']
                DATA_FILE = cfg['global']['data_file']
                KAFKA_TOPICS = cfg['global']['topic']
                LINGER_MS = cfg['producer']['linger_ms']
                DELAY_SWAP = cfg['global']['delay_swap']
                RECORDS_PER_MINUTE = cfg['global']['records_per_minute']
                BATCH_SIZE = cfg['producer']['batch_size']
                RECORD_CNT = cfg['global']['record_cnt']
                RUN_FLAG = cfg['global']['running']
                LOG_LEVEL = cfg['global']['log_level']
                
            producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER, linger_ms = LINGER_MS)

        if time_diff60 > 60:
            time_list60 = list(filter(lambda c: c >= datetime.datetime.now().timestamp() - 120, time_list60))
            exp_time60 = datetime.datetime.now().timestamp()
            
       
        msg = r
        msg.append(KAFKA_TOPICS)
        msg.append(records_per_second)
        msg.append(BATCH_SIZE)
        msg.append(LINGER_MS)    
        msg.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))    
        msg_flat = ';'.join(str(m) for m in msg)
        
        #print(msg_flat)
        
        if RUN_FLAG == 1:
            for t in KAFKA_TOPICS:
                kafka_python_producer_send(producer, msg_flat.encode(), t)
            cnt = cnt + 1
            time_list60.append(datetime.datetime.now().timestamp())
        
        #time.sleep(1/records_per_second)
        accurate_delay(1000/records_per_second)
        
        if (cnt > RECORD_CNT) and (RECORD_CNT > 0):
            logger.warning('Maximum number of events reached: %s', str(RECORD_CNT))
            break
        
    if (cnt > RECORD_CNT) and (RECORD_CNT > 0):
        logger.warning('Maximum number of events reached: %s', str(RECORD_CNT))
        break
    
    