from time import time

from redis import Redis

from util import display_results


class TimeSeriesWithHash(object):
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
                'hash_size': self._units['minute'] * 5
            },
            '1min': {
                'name': '1min',
                'ttl': self._units['day'] * 7,
                'duration': self._units['minute'],
                'hash_size': self._units['hour'] * 8
            },
            '1hour': {
                'name': '1hour',
                'ttl': self._units['day'] * 60,
                'duration': self._units['hour'],
                'hash_size': self._units['day'] * 10
            },
            '1day': {
                'name': '1day',
                'ttl': None,
                'duration': self._units['day'],
                'hash_size': self._units['day'] * 30
            },
        }

    def insert(self, timestamp_in_seconds):
        for name, granularity in self._granularities.items():
            key = self._key_name(granularity, timestamp_in_seconds)
            field_name = self._rounded_timestamp(timestamp_in_seconds, granularity['duration'])
            self._client.hincrby(key, field_name, 1)

            if (granularity['ttl'] is not None):
                self._client.expire(key, granularity['ttl'])

    def _key_name(self, granularity, timestamp_in_seconds):
        last_key_part = self._rounded_timestamp(timestamp_in_seconds, granularity['hash_size'])
        return ':'.join([self._namespace, granularity['name'], str(last_key_part)])

    def _rounded_timestamp(self, timestamp_in_seconds, precision):
        return int((timestamp_in_seconds // precision) * precision)

    def fetch(self, granularity_name, begin_timestamp, end_timestamp):
        granularity = self._granularities[granularity_name]
        duration = granularity['duration']
        begin = self._rounded_timestamp(begin_timestamp, duration)
        end = self._rounded_timestamp(end_timestamp, duration)
        keys = []

        timestamp = begin

        while timestamp <= end:
            key = self._key_name(granularity, timestamp)
            field_name = self._rounded_timestamp(timestamp, granularity['duration'])
            keys.append((key, field_name))
            timestamp += duration

        values = []
        for key, field_name in keys:
            values.append(self._client.hget(key, field_name))

        results = []
        for i, value in enumerate(values):
            timestamp = begin + i * duration
            results.append({'timestamp': timestamp, 'value': int(value)})

        return results


if __name__ == '__main__':
    client = Redis(port=9005, db=0)
    client.flushdb()
    ts = TimeSeriesWithHash(client, 'purchases:item1')

    ts.insert(0)
    ts.insert(1)
    ts.insert(1)
    ts.insert(3)
    ts.insert(61)

    results = ts.fetch('1day', 0, 4)
    display_results('1sec', results)
