import logging
from threading import Thread, Timer

import margay.stats as stats


class Schedule(Thread):
    def __init__(self, interval, reap_interval, database):
        super().__init__()
        self.logger = logging.getLogger()
        self._timer = None
        self.is_running = False
        self.counter = 0
        self.last_opened_connections = 0
        self.last_request_count = 0
        self.interval = interval
        self.reap_interval = reap_interval
        self.database = database
        self._reap = self.reap_interval
        self.start()

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.interval, self._run)
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False

    def _run(self):
        self.is_running = False
        self.start()

        self._reap -= self.interval

        if self.counter % 20 == 0:
            self.logger.info(f'{stats.open_connections} open, '
                             f'{stats.opened_connections} connections ({stats.connection_rate}/s) '
                             f'{stats.requests} requests ({stats.request_rate}/s)')

        self.last_opened_connections = stats.opened_connections
        self.last_request_count = stats.requests

        self.database.flush()

        if self._reap <= 0:
            self._reap = self.reap_interval

        self.counter += 1
