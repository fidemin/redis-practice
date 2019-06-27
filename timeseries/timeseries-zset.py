from time import time

from redis import Redis

from util import display_results


class TimeSeriesWithZset(object):
    def __init__(self, client, namespace):
        self._namespace = namespace
        self._client = client
        self._units = {
            'second': 1,
            'minute': 60,
            'hour': 60 * 60,
            'day': 24 * 60 * 60
        }
        self._granularities = {
            '1sec': {
                'name': '1sec',
                'ttl': self._units['hour'] * 2,
                'duration': self._units['second'],
                'hash_size': self._units['minute'] * 2
            },
            '1min': {
                'name': '1min',
                'ttl': self._units['day'] * 7,
                'duration': self._units['minute'],
                'hash_size': self._units['hour'] * 2
            },
            '1hour': {
                'name': '1hour',
                'ttl': self._units['day'] * 60,
                'duration': self._units['hour'],
                'hash_size': self._units['day'] * 5
            },
            '1day': {
                'name': '1day',
                'ttl': None,
                'duration': self._units['day'],
                'hash_size': self._units['day'] * 30
            },
        }

    def insert(self, timestamp_in_seconds, thing):
        for name, granularity in self._granularities.items():
            key = self._key_name(granularity, timestamp_in_seconds)
            timestamp_score = self._rounded_timestamp(timestamp_in_seconds, granularity['duration'])
            member = str(timestamp_score) + ':' + thing
            self._client.zadd(key, {member: timestamp_score})

            if (granularity['ttl'] is not None):
                self._client.expire(key, granularity['ttl'])

    def fetch(self, granularity_name, begin_timestamp, end_timestamp):
        granularity = self._granularities[granularity_name]
        duration = granularity['duration']
        begin = self._rounded_timestamp(begin_timestamp, duration)
        end = self._rounded_timestamp(end_timestamp, duration)
        keys = []

        timestamp = begin

        results = []
        while timestamp <= end:
            key = self._key_name(granularity, timestamp)
            value= self._client.zcount(key, timestamp, timestamp)
            results.append({'timestamp': timestamp, 'value':value})
            timestamp += duration

        return results

    def _key_name(self, granularity, timestamp_in_seconds):
        last_key_part = self._rounded_timestamp(timestamp_in_seconds, granularity['hash_size'])
        return ':'.join([self._namespace, granularity['name'], str(last_key_part)])

    def _rounded_timestamp(self, timestamp_in_seconds, precision):
        return int((timestamp_in_seconds // precision) * precision)


if __name__ == '__main__':
    client = Redis(port=9005, db=0)
    client.flushdb()

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
