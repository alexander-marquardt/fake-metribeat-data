import random
import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

NUMBER_OF_DAYS_TO_INSERT =100
TODAY_DATE = datetime.datetime.now()
START_DATE = TODAY_DATE - datetime.timedelta(days=NUMBER_OF_DAYS_TO_INSERT)
TIME_BETWEEN_INSERTS = datetime.timedelta(seconds=15)
CPU_SPIKE_EVERY_DAYS = datetime.timedelta(days=20)
RANDOMIZE_SPIKE_DAYS = 20  # move the spike forward and backward in time by a random amount
SPIKE_DURATION_HOURS = datetime.timedelta(hours=96) # hours

ES_HOST = 'localhost:9200'
ES_USER = 'elastic'
ES_PASSWORD = 'elastic'

FAKE_METRICBEAT_INDEX_NAME = "fake-metricbeat-idx"
CPU_FIELDNAME = "system.cpu.total.pct"
CPU_NORMAL_VALUE = 2 # 200%
ADD_RANDOM_TO_CPU_FOR_HIGH_VALUE = 1  # add from zero to this value
CPU_VARIANCE = 0.5  # plus or minus


SETTINGS = {
    'index': {
        'refresh_interval': '10s',
        'number_of_shards': 1,
        'number_of_replicas': 0,
    }
}

MAPPINGS = {
    'properties': {
        '@timestamp': {
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


def get_cpu_spike_vars(curr_date):
    cpu_spike_start = curr_date + CPU_SPIKE_EVERY_DAYS + datetime.timedelta(
        days=random.uniform(-RANDOMIZE_SPIKE_DAYS, RANDOMIZE_SPIKE_DAYS))
    cpu_spike_end = cpu_spike_start + SPIKE_DURATION_HOURS
    cpu_spike_amount = random.uniform(-ADD_RANDOM_TO_CPU_FOR_HIGH_VALUE, ADD_RANDOM_TO_CPU_FOR_HIGH_VALUE)
    return cpu_spike_start, cpu_spike_end, cpu_spike_amount


def insert_fake_cpu_docs():
    es = Elasticsearch([ES_HOST], http_auth=(ES_USER, ES_PASSWORD))

    print(f"Deleting index {FAKE_METRICBEAT_INDEX_NAME}")
    es.indices.delete(index=FAKE_METRICBEAT_INDEX_NAME, ignore=[400, 404])
    
    request_body = {
        'settings': SETTINGS,
        'mappings': MAPPINGS,
    }

    print(f"Creating index {FAKE_METRICBEAT_INDEX_NAME}")
    es.indices.create(index=FAKE_METRICBEAT_INDEX_NAME, body=request_body)

    # docs_for_bulk_insert - an array to collect documents for bulk insertion
    docs_for_bulk_insert = []
    # bulk_counter - track how many documents are in the actions array
    bulk_counter = 0

    print("Bulk inserting from %s to %s\n" % (START_DATE, TODAY_DATE))
    curr_date = START_DATE
    cpu_spike_start, cpu_spike_end, cpu_spike_amount = get_cpu_spike_vars(curr_date)

    while curr_date <= TODAY_DATE:

        if cpu_spike_start < curr_date < cpu_spike_end:
            cpu_val = CPU_NORMAL_VALUE + cpu_spike_amount + random.uniform(-CPU_VARIANCE, CPU_VARIANCE)
        else:
            cpu_val = CPU_NORMAL_VALUE + random.uniform(-CPU_VARIANCE, CPU_VARIANCE)

            if cpu_spike_start < curr_date:
                cpu_spike_start, cpu_spike_end, cpu_spike_amount = get_cpu_spike_vars(curr_date)

        action = {
            '_index': FAKE_METRICBEAT_INDEX_NAME,
            '_id': None,
            '_source': {
                CPU_FIELDNAME: '%s' % cpu_val,
                '@timestamp': curr_date
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
