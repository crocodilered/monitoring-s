import elasticsearch
import matplotlib.pyplot as plt


if __name__ == '__main__':
    es = elasticsearch.Elasticsearch()

    # QUERY ############################################################################################################

    res = es.search(index='logs', body={
        'size': 0,
        'query': {
            'range': {
                'timestamp': {
                    'gte': '2019-03-01T00:00:00',
                    'lte': '2019-04-01T00:00:00'
                }
            }
        },
        'aggs': {
            'hosts': {
                'terms': {'field': 'host.keyword'},
                "aggs": {
                    'dt_histogram': {
                        "date_histogram": {
                            "field": "timestamp",
                            "calendar_interval": "day",
                            'min_doc_count': 0,
                        },
                        'aggs': {
                            "sum_bytes": {
                                "sum": {
                                    "field": "bytes"
                                }
                            }
                        }
                    },

                }
            }
        }
    })

    # CSV OUT ##########################################################################################################

    table = []

    for x in res['aggregations']['hosts']['buckets']:
        for y in x['dt_histogram']['buckets']:
            table.append((
                y['key_as_string'][:10],
                x['key'],
                str(int(y['sum_bytes']['value']))
            ))

    table.sort(key=lambda tup: (tup[0], tup[1]))

    print(';'.join(['Date', 'Host', 'Bytes']))

    for x in table:
        print(';'.join(x))

    del table

    # PLOT FUN #########################################################################################################

    hosts_buckets = res['aggregations']['hosts']['buckets']

    labels, bottom = [], []
    for x in hosts_buckets[0]['dt_histogram']['buckets']:
        labels.append(x['key_as_string'][8:10])
        bottom.append(0)

    hosts, means = [], {}

    for x in hosts_buckets:
        h = x['key']
        hosts.append(h)
        means[h] = []
        for y in x['dt_histogram']['buckets']:
            means[h].append(y['sum_bytes']['value'])

    width = 0.4

    fig, ax = plt.subplots()

    for x in hosts:
        ax.bar(labels, means[x], width, label=x)

        for i, b in enumerate(means[x]):
            bottom[i] += b

    ax.set_xlabel('Dates')
    ax.set_ylabel('Sums of bytes')
    ax.legend()

    plt.show()
