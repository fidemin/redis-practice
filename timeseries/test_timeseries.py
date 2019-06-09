"""
Test with pytest
"""
from timeseries import TimeSeries

class MockRedisClient(object):
    def __init__(self):
        self._data = {}

    def incr(self, key):
        if self._exist(key):
            value = float(self._get(key))
            value += 1
            self._set(key, value)
            return
        self._set(key, 1)

    def _exist(self, key):
        try:
            self._data[key]
        except KeyError:
            return False
        else:
            return True

    def _get(self, key):
        return self._data.get(key, None)

    def _set(self, key, value):
        self._data[key] = str(value).encode('utf-8')

    def expire(self, key, ttl_in_seconds):
        pass

    def mget(self, keys):
        return [self._data.get(key, None) for key in keys]


class TestTimeSeries(object):
    def setup_method(self):
        self.client = MockRedisClient()
        self.timeseries = TimeSeries(self.client, 'test')
        timestamps = [0, 0, 1, 1, 1, 60]

        for timestamp in timestamps:
            self.timeseries.insert(timestamp)

    def teardown_method(self):
        pass

    def test_insert(self):
        assert int(float(self.client._data['test:1sec:0'])) == 2
        assert int(float(self.client._data['test:1sec:1'])) == 3
        assert int(float(self.client._data['test:1sec:60'])) == 1
        assert int(float(self.client._data['test:1min:0'])) == 5
        assert int(float(self.client._data['test:1min:60'])) == 1
        assert int(float(self.client._data['test:1hour:0'])) == 6
        assert int(float(self.client._data['test:1day:0'])) == 6

    def test_fetch(self):
        begin_timestamp = 0
        end_timestamp = 60

        results = self.timeseries.fetch('1min', begin_timestamp, end_timestamp)
        assert results[0]['timestamp'] == 0
        assert int(float(results[0]['value'])) == 5
        assert int(float(results[1]['timestamp'])) == 60
        assert int(float(results[1]['value'])) == 1

