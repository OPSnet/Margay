from argparse import ArgumentParser
import logging
from logging.handlers import TimedRotatingFileHandler
import signal
import sys
import threading

from . import __version__
from .config import Config
from .database import Database
from .site_comm import SiteComm
from .schedule import Schedule
from .worker import Worker


def run():
    parser = ArgumentParser(description='Python BitTorrent tracker')
    parser.add_argument('-d', '--daemonize', action='store_true', help='Run tracker as daemon')
    parser.add_argument('-c', '--config', nargs='?')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s ' + __version__)
    args = parser.parse_args()
    config = Config(args.config, args.daemonize)

    logger = logging.getLogger()
    while logger.handlers:
        logger.handlers.pop()
    log_format = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    if config['logging']['log']:
        if config['logging']['log_file']:
            file_logger = TimedRotatingFileHandler(config['logging']['log_path'], when='d',
                                                   backupCount=5)
            file_logger.setFormatter(log_format)
            file_logger.setLevel(config['logging']['log_level'])
            #logger.addHandler(file_logger)
        if config['logging']['log_console']:
            console_logger = logging.StreamHandler(sys.stdout)
            console_logger.setFormatter(log_format)
            console_logger.setLevel(config['logging']['log_level'])
            logger.addHandler(console_logger)
    else:
        logger.addHandler(logging.NullHandler())
    logger.setLevel(config['logging']['log_level'])

    database = Database(config['mysql'], config['debug']['readonly'])
    schedule = Schedule(config['timers']['schedule_interval'],
                        config['timers']['reap_peers_interval'],
                        database)
    site_comm = SiteComm(config)
    worker = Worker(database, site_comm, config)

    def sig_handler(sig, _):
        print("help")
        logger = logging.getLogger()
        if sig == signal.SIGINT or sig == signal.SIGTERM:
            logger.info('Caught SIGINT/SIGTERM')
            if worker.shutdown():
                raise SystemExit
        elif sig == signal.SIGHUP:
            logger.info('Reloading config')
            config.reload()
            # reload various classes
        elif sig == signal.SIGUSR1:
            logger.info('Reloading from database')
            threading.Thread(target=worker.reload_lists)

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGHUP, sig_handler)
    signal.signal(signal.SIGUSR1, sig_handler)
    signal.signal(signal.SIGUSR2, sig_handler)

    try:
        worker.create_server(config['internal']['listen_port'])
    finally:
        schedule.stop()
