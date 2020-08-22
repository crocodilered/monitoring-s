import elasticsearch


if __name__ == '__main__':
    es = elasticsearch.Elasticsearch()

    res = es.search(index='flights', body={
        'size': 0,
        'query': {
            'bool': {
                'must_not': {'term': {'DistanceKilometers': 0}}
            }
        },
        'aggs': {
            'carriers': {
                'terms': {'field': 'Carrier.keyword'},
                'aggs': {
                    'avg_delay': {
                        'avg': {'field': 'FlightDelayMin'}
                    }
                }
            }
        }
    })

    print(';'.join(['Carrier', 'Average FlightDelayMin']))

    for x in res['aggregations']['carriers']['buckets']:
        print(';'.join([
            x['key'],
            '{:.3f}'.format(x['avg_delay']['value'])
        ]))
