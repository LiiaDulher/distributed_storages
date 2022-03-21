import hazelcast


class DistributedMap:

    def __init__(self, client):
        self.map = client.get_map("DistributedMap").blocking()

    def __getitem__(self, key):
        return self.map.get(key)

    def __setitem__(self, key, value):
        self.map.put(key, value)

    def __delitem__(self, key):
        self.map.delete(key)

    def __contains__(self, key):
        return self.map.contains_key(key)

    def __len__(self):
        return self.map.size()

    def __repr__(self):
        return self.map.values()
