import elasticsearch
import os
import ijson


def create_index(elastic_instance, index, file_path):
    print('Start to create index %s' % index)

    elastic_instance.indices.delete(index=index, ignore=[400, 404])

    with open(file_path, 'r', encoding='utf-8') as f:
        c = 0
        for doc in ijson.items(f, 'item'):
            if c % 100 == 0:
                print('-%s' % c)
            c += 1

            elastic_instance.index(index=index, body=doc)


if __name__ == '__main__':
    es = elasticsearch.Elasticsearch()

    create_index(es, 'flights', os.path.join('data', 'flights.json'))
    create_index(es, 'logs', os.path.join('data', 'logs.json'))
