import random
import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers


NUMBER_OF_DAYS_TO_INSERT =100
TODAY_DATE = datetime.datetime.now()
START_DATE = TODAY_DATE - datetime.timedelta(days=NUMBER_OF_DAYS_TO_INSERT)
TIME_BETWEEN_INSERTS = datetime.timedelta(seconds=15)
CPU_SPIKE_EVERY_DAYS = datetime.timedelta(days=25)
RANDOMIZE_SPIKE_HOURS = 120 # move the spike forward and backward in time
SPIKE_DURATION_HOURS = datetime.timedelta(hours=96) # hours


ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

FAKE_METRICBEAT_INDEX_NAME = "fake-metricbeat-idx"
CPU_FIELDNAME = "system.cpu.total.pct"
CPU_NORMAL_VALUE = 1.60 # 160 %
CPU_HIGH_VALUE = 3.50
CPU_VARIANCE = 0.4 # plus or minus


SETTINGS = {
    'index': {
        'refresh_interval': '10s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
    }
}

MAPPINGS = {
    'properties': {
        'timestamp': {
            'type': 'date'
        },
        CPU_FIELDNAME: {
            "type": "scaled_float",
            "scaling_factor": 1000.0
            }
        }
    }

ONE_THOUSAND = 1000
BULK_SIZE = 1 * ONE_THOUSAND


def insert_fake_cpu_docs():
    es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))

    print(f"Deleting index {FAKE_METRICBEAT_INDEX_NAME}")
    es.indices.delete(index=FAKE_METRICBEAT_INDEX_NAME, ignore=[400, 404])
    
    request_body = {
        'settings': SETTINGS,
        'mappings': MAPPINGS,
    }

    print(f"Deleting index {FAKE_METRICBEAT_INDEX_NAME}")
    es.indices.create(index=FAKE_METRICBEAT_INDEX_NAME, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    print("%s Starting bulk insertion of documents\n" % datetime.datetime.now().isoformat())
    curr_date = START_DATE
    cpu_spike_start = curr_date + CPU_SPIKE_EVERY_DAYS + datetime.timedelta(
        hours=random.uniform(-RANDOMIZE_SPIKE_HOURS, RANDOMIZE_SPIKE_HOURS))
    cpu_spike_end = cpu_spike_start + SPIKE_DURATION_HOURS

    while curr_date <= TODAY_DATE:

        if cpu_spike_start < curr_date < cpu_spike_end:
            cpu_val = CPU_HIGH_VALUE + random.uniform(-CPU_VARIANCE, CPU_VARIANCE)
        else:
            cpu_val = CPU_NORMAL_VALUE + random.uniform(-CPU_VARIANCE, CPU_VARIANCE)

            if cpu_spike_start < curr_date:
                cpu_spike_start = curr_date + CPU_SPIKE_EVERY_DAYS + datetime.timedelta(
                    hours=random.uniform(-RANDOMIZE_SPIKE_HOURS, RANDOMIZE_SPIKE_HOURS))
                cpu_spike_end = cpu_spike_start + SPIKE_DURATION_HOURS

        action = {
            '_index': FAKE_METRICBEAT_INDEX_NAME,
            '_id': None,
            '_source': {
                CPU_FIELDNAME: '%s' % cpu_val,
                'timestamp': curr_date
                }
            }
        docs_for_bulk_insert.append(action)

        bulk_counter += 1
        if bulk_counter >= BULK_SIZE:
            helpers.bulk(es, docs_for_bulk_insert)
            docs_for_bulk_insert = []
            bulk_counter = 0

        curr_date += TIME_BETWEEN_INSERTS


if __name__ == "__main__":
    # execute only if run as a script
    insert_fake_cpu_docs()
    print("Finished")
