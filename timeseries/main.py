
from timeseries import TimeSeries
from redis import Redis

if __name__ == '__main__':
    client = Redis(port=9005, db=0) 
    ts = TimeSeries(client, 'purchases:item1')

    ts.insert(0)
    ts.insert(1)
    ts.insert(1)
    ts.insert(3)
    ts.insert(61)

    results = ts.fetch('1sec', 0, 4)
    print(results)
