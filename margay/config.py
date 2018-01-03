"""
Configuration Class
"""

from configparser import ConfigParser
import logging


class Config(object):
    def __init__(self, config_file=None, daemonize=False):
        self.config = {
            'internal': {
                'listen_port': 35000,
                'max_connections': 1024,
                'max_middlemen': 20000,
                'max_read_buffer': 4096,
                'connection_timeout': 10,
                'keepalive_timeout': 0,
                'daemonize': daemonize
            },
            'tracker': {
                'announce_interval': 1800,
                'max_request_size': 4096,
                'numwant_limit': 50,
                'request_log_size': 500
            },
            'timers': {
                'del_reason_lifetime': 86400,
                'peers_timeout': 7200,
                'reap_peers_interval': 1800,
                'schedule_interval': 3
            },
            # note: host=localhost will cause mysqlclient to use a socket regardless of port,
            # use 127.0.0.1 for the host if you're trying to connect to something with a port
            'mysql': {
                'host': '127.0.0.1',
                'db': 'gazelle',
                'user': 'gazelle',
                'passwd': 'password',
                'port': 36000
            },
            'gazelle': {
                'site_host': '127.0.0.1',
                'site_path': '',
                'site_password': '00000000000000000000000000000000',
                'report_password': '00000000000000000000000000000000'
            },
            'logging': {
                'log': True,
                'log_level': logging.getLevelName(logging.INFO),
                'log_console': True,
                'log_file': False,
                'log_path': '/tmp/margay'
            },
            'debug': {
                'readonly': False
            }
        }
        print(self.config)

        if config_file is not None:
            config = ConfigParser()
            config.read(config_file)
            for key in config:
                for value in config[key]:
                    if type(self.config[key][value]) == int:
                        self.config[key][value] = int(self.config[key][value])
                    elif type(self.config[key][value]) == bool:
                        self.config[key][value] = config[key][value] in ('True', 'true', 'On',
                                                                         'on')
                    else:
                        self.config[key][value] = config[key][value]
        if self.config['logging']['log_level'] == 'debug':
            self.print()

    def reload(self):
        pass

    def print(self):
        for key in self.config:
            print(self.config[key])
            for kkey in self.config[key]:
                print(kkey, self.config[key][kkey])

    def __getitem__(self, item):
        return self.config[item]
