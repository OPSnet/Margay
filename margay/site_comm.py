import logging
import threading

import requests


class SiteComm(object):
    def __init__(self, config):
        self.config = config

        self.logger = logging.getLogger()

        self.site_host = ''
        self.site_path = ''
        self.site_password = ''
        self.readonly = False

        self.expire_queue_lock = threading.RLock()
        self.token_queue = list()
        self.expire_token_buffer = ''
        self.t_active = False
        self.verbose_flush = False

        self.load_config(self.config)

    def load_config(self, config):
        self.site_host = config['gazelle']['site_host']
        self.site_path = config['gazelle']['site_path']
        self.site_password = config['gazelle']['site_password']
        self.readonly = config['debug']['readonly']

    def reload_config(self, config):
        self.load_config(config)

    def all_clear(self) -> bool:
        return len(self.token_queue) == 0

    def expire_token(self, torrent: int, user: int):
        token_pair = f'{user}:{torrent}'
        if self.expire_token_buffer != '':
            self.expire_token_buffer += ','
        self.expire_token_buffer += token_pair
        if len(self.expire_token_buffer) > 350:
            self.logger.info('Flushing overloaded token buffer')
            if not self.readonly:
                with self.expire_queue_lock:
                    self.token_queue.append(self.expire_token_buffer)
            self.expire_token_buffer = ''

    def flush_tokens(self) -> None:
        if self.readonly:
            self.expire_token_buffer = ''
            return
        with self.expire_queue_lock:
            if self.verbose_flush or len(self.token_queue) > 0:
                self.logger.info(f'Token expire queue size: {len(self.token_queue)}')
            if self.expire_token_buffer == '':
                return
            self.token_queue.extend(self.expire_token_buffer)
            self.expire_token_buffer = ''
            if not self.t_active:
                threading.Thread(target=self._do_flush_tokens)

    def _do_flush_tokens(self):
        self.t_active = True
        while len(self.token_queue) > 0:
            response = requests.get(f'https://{self.site_host}/tools.php', params={
                'key': self.site_password,
                'type': 'expiretoken',
                'action': 'ocelot',
                'tokens': self.token_queue[0]
            })

            if response.status_code == 200:
                with self.expire_queue_lock:
                    self.token_queue.pop(0)
            else:
                self.logger.error(f'Response returned with status code {response.status_code} '
                                  f'when trying to expire a token!')
        self.t_active = False
