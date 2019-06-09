
from timeseries import TimeSeries, TimeSeriesWithHash
from redis import Redis

if __name__ == '__main__':
    client = Redis(port=9005, db=0) 
    client.flushdb()
    #ts = TimeSeries(client, 'purchases:item1')
    ts = TimeSeriesWithHash(client, 'purchases:item1')

    ts.insert(0)
    ts.insert(1)
    ts.insert(1)
    ts.insert(3)
    ts.insert(61)

    results = ts.fetch('1day', 0, 4)
    print(results)
