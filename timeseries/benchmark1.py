
from redis import Redis

from timeseries import TimeSeriesWithHash

client = Redis(port=9005, db=0)
client.flushdb()
ts = TimeSeriesWithHash(client, 'test')
before = client.info(section='memory')

for timestamp in range(60 * 60):
    ts.insert(timestamp)

after = client.info(section='memory')

print(after['used_memory'] - before['used_memory'])
