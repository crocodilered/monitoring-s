import elasticsearch
import matplotlib.pyplot as plt


def show_csv(buckets):
    csv_table = []

    for x in buckets:
        for y in x['dt_histogram']['buckets']:
            csv_table.append((
                y['key_as_string'][:10],
                x['key'],
                str(int(y['sum_bytes']['value']))
            ))

    csv_table.sort(key=lambda tup: (tup[0], tup[1]))

    print(';'.join(['Date', 'Host', 'Bytes']))

    for row in csv_table:
        print(';'.join(row))


def show_histogram(buckets):
    fig, ax = plt.subplots()

    dates, bottom, sums = [], [], {}

    for i, x in enumerate(buckets):
        host = x['key']
        sums[host] = []

        for y in x['dt_histogram']['buckets']:
            sums[host].append(y['sum_bytes']['value'])

            if i == 0:
                dates.append(y['key_as_string'][8:10])
                bottom.append(0)

        ax.bar(dates, sums[host], 0.5, label=host)

        for i, b in enumerate(sums[host]):
            bottom[i] += b

    ax.set_xlabel('Month days')
    ax.set_ylabel('Bytes sums')

    ax.legend()

    plt.show()


if __name__ == '__main__':
    es = elasticsearch.Elasticsearch()

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

    show_csv(res['aggregations']['hosts']['buckets'])
    show_histogram(res['aggregations']['hosts']['buckets'])
