
from collections import Mapping


class MapWrapper(Mapping):

    def __init__(self, internal_mapping):
        self._map = internal_mapping

    def __iter__(self):
        return iter(self._map)

    def __contains__(self, value):
        return value in self._map

    def __len__(self):
        return len(self._map)

    def __getitem__(self, key):
        return self._map[key]
