
from timeseries import TimeSeries, TimeSeriesWithHash, TimeSeriesWithZset
from redis import Redis

def timeseries(client):
    ts = TimeSeries(client, 'purchases:item1')

    ts.insert(0)
    ts.insert(1)
    ts.insert(1)
    ts.insert(3)
    ts.insert(61)

    results = ts.fetch('1day', 0, 4)
    print(results)


def timeseries_with_hash(client):
    ts = TimeSeriesWithHash(client, 'purchases:item1')

    ts.insert(0)
    ts.insert(1)
    ts.insert(1)
    ts.insert(3)
    ts.insert(61)

    results = ts.fetch('1day', 0, 4)
    print(results)


def display_results(granularity_name, results):
    print('Results from ' + granularity_name + ':')
    print('Timestamp\t| Value')
    print('-------- | ------')
    for result in results:
        print(str(result['timestamp']) + '\t| ' + str(result['value']))


def timeseries_with_zset(client):
    ts = TimeSeriesWithZset(client, 'concurrentplays')
    begin_timestamp = 0
    ts.insert(begin_timestamp, 'user:max')
    ts.insert(begin_timestamp, 'user:max')
    ts.insert(begin_timestamp+1, 'user:hugo')
    ts.insert(begin_timestamp+1, 'user:renata')
    ts.insert(begin_timestamp+3, 'user:hugo')
    ts.insert(begin_timestamp+61, 'user:kc')

    results = ts.fetch('1sec', begin_timestamp, begin_timestamp+4)
    display_results('1sec', results)


if __name__ == '__main__':
    client = Redis(port=9005, db=0)
    client.flushdb()
    timeseries_with_zset(client)
