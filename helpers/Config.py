import yaml


class Config:
    def __init__(self, file):
        with open(file, 'r') as stream:
            try:
                self.config = yaml.safe_load(stream)
            except yaml.YAMLError:
                raise ConfigException('Not found configuration')

    def get(self, name=None, default=None):
        if name is None:
            return self.config

        if name not in self.config:
            return default

        return self.config[name]


class ConfigException(Exception):
    pass
