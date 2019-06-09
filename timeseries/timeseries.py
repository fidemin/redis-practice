from time import time

class TimeSeries(object):
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
                'duration': self._units['second']
            },
            '1min': {
                'name': '1min',
                'ttl': self._units['day'] * 7,
                'duration': self._units['minute']
            },
            '1hour': {
                'name': '1hour',
                'ttl': self._units['day'] * 60,
                'duration': self._units['hour']
            },
            '1day': {
                'name': '1day',
                'ttl': None,
                'duration': self._units['day']
            },
        }

    def insert(self, timestamp_in_seconds):
        for name, granularity in self._granularities.items():
            key = self._key_name(granularity, timestamp_in_seconds)
            self._client.incr(key)

            if (granularity['ttl'] is not None):
                self._client.expire(key, granularity['ttl'])

    def _key_name(self, granularity, timestamp_in_seconds):
        rounded_timestamp = self._rounded_timestamp(timestamp_in_seconds, granularity['duration'])
        return ':'.join([self._namespace, granularity['name'], str(rounded_timestamp)])

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
            keys.append(key)
            timestamp += duration

        values = self._client.mget(keys)

        results = []
        for i, value in enumerate(values):
            timestamp = begin + i * duration
            results.append({'timestamp': timestamp, 'value': value})

        return results


if __name__ == '__main__':
    time_series = TimeSeries(None, 'test')

