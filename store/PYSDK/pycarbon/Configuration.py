class Configuration(object):
    def __init__(self, java_gate_way):
        self.conf = java_gate_way.entry_point.createConfiguration()

    def set(self, key, value):
        self.conf.set(key, value)
        return self.conf
