class Configuration(object):
    def __init__(self):
        from jnius import autoclass
        ConfigurationClass = autoclass('org.apache.hadoop.conf.Configuration')
        self.conf = ConfigurationClass()

    def set(self, key, value):
        self.conf.set(key, value)
        return self.conf
