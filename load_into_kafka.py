import configparser
import json
from time import sleep

from confluent_kafka import Producer


def read_config(section):
    config = configparser.ConfigParser()
    config_dict = {}
    try:
        config.read('config.txt')
        for key in config[section]:
            config_dict[key] = config[section][key]

        return config_dict
    except Exception as ex:
        print(f"Error reading config file : {ex}")
        return None

def produce_kafka(file, producer, topic):
    with open(file) as fin:
        for line in fin:
            value = json.loads(line)
            key = value['trans_num']
            producer.produce(topic, key=key, value = json.dumps(value).encode('utf-8'), on_delivery = callback)
            producer.poll()
            sleep(1)

    producer.flush()



def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        print(f'Value for {event.key()} sent to partition {event.partition()}.')

if __name__ == "__main__" :
    configs = read_config('kafka')
    producer_config = {
        'bootstrap.servers': configs['kafka_bootstrap_servers']
    }
    kafka_producer = Producer(producer_config)

    try:
        produce_kafka(configs['kafka_source_file'], kafka_producer, configs['kafka_topic'])

    except KeyboardInterrupt:
        print("User terminated the load!")
    except Exception as e:
        print(f"Unknown error occurred!! Error = {e}")
