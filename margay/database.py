import binascii
from copy import copy
import logging
from time import time
from typing import Dict
import threading
# noinspection PyPackageRequirements
import MySQLdb

from .structs import Torrent, User, LeechType
import margay.stats as stats


class Database(object):
    def __init__(self, settings, readonly=False):
        self.logger = logging.getLogger()
        self.settings = settings
        self.db = self.get_connection()

        self.readonly = readonly

        self.user_buffer = []
        self.torrent_buffer = []
        self.heavy_peer_buffer = []
        self.light_peer_buffer = []
        self.snatch_buffer = []
        self.token_buffer = []

        self.user_queue = []
        self.torrent_queue = []
        self.peer_queue = []
        self.snatch_queue = []
        self.token_queue = []

        self.u_active = self.t_active = self.p_active = self.s_active = self.tok_active = False
        self.user_lock = threading.RLock()
        self.torrent_lock = threading.RLock()
        self.peer_lock = threading.RLock()
        self.snatch_lock = threading.RLock()
        self.token_lock = threading.RLock()

        self.torrent_list_lock = threading.RLock()
        self.user_list_lock = threading.RLock()
        self.whitelist_lock = threading.RLock()

        if not self.readonly:
            self.logger.info('Clearing xbt_files_users and resetting peer counts...')
            self.flush()
            self._clear_peer_data()
            self.logger.info('done')

    def get_connection(self):
        return MySQLdb.connect(host=self.settings['host'], user=self.settings['user'],
                               passwd=self.settings['passwd'], db=self.settings['db'],
                               port=self.settings['port'])

    def connected(self):
        return self.db is not None

    def load_torrents(self, torrents=None):
        if torrents is None:
            torrents = dict()
        cur_keys = set(torrents.keys())

        cursor = self.db.cursor()
        # info_hash is a binary blob and using HEX converts it to a hex string and LCASE lowers it
        # so that we don't have to worry about weird cases
        cursor.execute('SELECT ID, info_hash, FreeTorrent, Snatched FROM torrents '
                       'ORDER BY ID')
        with self.torrent_list_lock:
            for row in cursor.fetchall():
                info_hash = row[1].decode('utf-8', 'replace')
                if info_hash == '':
                    continue
                if info_hash not in torrents:
                    torrents[info_hash] = Torrent(row[0], row[3])
                else:
                    torrents[info_hash].tokened_users.clear()
                    cur_keys.remove(info_hash)
                torrents[info_hash].free_torrent = LeechType.to_enum(row[2])
            cursor.close()

            for key in cur_keys:
                stats.leechers -= torrents[key].leechers
                stats.seeders -= torrents[key].seeders
                for leecher in torrents[key].leechers:
                    leecher.user.leeching -= 1
                for seeder in torrents[key].seeders:
                    seeder.user.seeding -= 1
                del torrents[key]

        self.logger.info(f'Loaded {len(torrents)} torrents')
        self.load_tokens(torrents)
        return torrents

    def load_users(self, users=None):
        if users is None:
            users = dict()
        cur_keys = set(users.keys())

        cursor = self.db.cursor()
        cursor.execute("SELECT ID, can_leech, torrent_pass, (Visible='0' OR IP='127.0.0.1') AS "
                       "Protected FROM users_main WHERE Enabled='1'")
        with self.user_list_lock:
            for row in cursor.fetchall():
                if row[2] not in users:
                    users[row[2]] = User(row[0], row[1], row[3])
                else:
                    users[row[2]].leech = row[1]
                    users[row[2]].protect = row[3]
                    cur_keys.remove(row[2])
            cursor.close()

            for key in cur_keys:
                del users[key]

        self.logger.info(f'Loaded {len(users)} users')
        return users

    def load_tokens(self, torrents: Dict[str, Torrent]):
        """

        :param torrents:
        :type torrents: Dict[str, Torrent]
        :return:
        """
        cursor = self.db.cursor()
        cursor.execute("SELECT uf.UserID, t.info_hash FROM users_freeleeches AS uf "
                       "JOIN torrents AS t ON t.ID = uf.TorrentID "
                       "WHERE uf.Expired = '0'")
        for row in cursor.fetchall():
            info_hash = str(row[1])
            torrent = torrents[info_hash]
            torrent.tokened_users.append(row[0])
        logging.info(f'Loaded {cursor.rownumber} tokens')
        cursor.close()

    def load_whitelist(self):
        cursor = self.db.cursor()
        whitelist = list()
        cursor.execute("SELECT peer_id FROM xbt_client_whitelist")
        with self.whitelist_lock:
            for result in cursor.fetchall():
                whitelist.append(result[0])
            cursor.close()

        if len(whitelist) == 0:
            self.logger.info('Assuming no whitelist desired, disabled')
        else:
            self.logger.info(f'Loaded {len(whitelist)} clients into the whitelist')
        return whitelist

    def record_token(self, user_id, torrent_id, downloaded):
        self.token_buffer.append((user_id, torrent_id, downloaded))

    def record_user(self, user_id, uploaded, downloaded):
        self.user_buffer.append((user_id, uploaded, downloaded))

    def record_torrent(self, torrent_id, seeders, leechers, snatched, balance):
        self.torrent_buffer.append((torrent_id, seeders, leechers, snatched, balance))

    def record_snatch(self, user_id, torrent_id, ipv4, ipv6):
        self.snatch_buffer.append((user_id, torrent_id, ipv4, ipv6))

    def record_peer_light(self, user_id, torrent_id, timespent, announced, peer_id):
        self.light_peer_buffer.append((user_id, torrent_id, timespent, announced, peer_id,
                                       int(time())))

    def record_peer_heavy(self, user_id, torrent_id, active, uploaded, downloaded, upspeed,
                          downspeed, remaining, corrupt, timespent, announced, ip, peer_id,
                          user_agent):
        self.heavy_peer_buffer.append((user_id, torrent_id, active, uploaded, downloaded, upspeed,
                                       downspeed, remaining, corrupt, timespent, announced,
                                       ip, peer_id, user_agent, int(time())))

    def flush(self):
        self._flush_users()
        self._flush_torrents()
        self._flush_snatches()
        self._flush_peers()
        self._flush_tokens()

    def _flush_users(self):
        if self.readonly:
            self.user_buffer.clear()
            return

        with self.user_lock:
            if len(self.user_queue) > 0:
                self.logger.info(f'User flush queue size: {len(self.user_queue)}, '
                                 f'next query length: {len(str(self.token_queue[0]))}')
            if len(self.user_buffer) == 0:
                return
            self.user_queue.extend(copy(self.user_buffer))
            self.user_buffer.clear()
        if not self.u_active:
            threading.Thread(target=self._do_flush_users).start()

    def _do_flush_users(self):
        self.u_active = True
        conn = self.get_connection()
        while len(self.user_queue) > 0:
            cursor = conn.cursor()
            cursor.executemany('INSERT INTO users_main (ID, Uploaded, Downloaded) '
                               'VALUES(%s, %s, %s) '
                               'ON DUPLICATE KEY UPDATE Uploaded = Uploaded + Values(Uploaded), '
                               'Downloaded = Downloaded + Values(Downloaded)', self.user_queue[0])
            cursor.close()
            with self.user_lock:
                self.user_queue.pop(0)
        self.u_active = False

    def _flush_torrents(self):
        if self.readonly:
            self.torrent_buffer.clear()
            return

        with self.torrent_lock:
            if len(self.torrent_queue) > 0:
                self.logger.info(f'Torrent flush queue size: {len(self.torrent_queue)}, '
                                 f'next query length: {len(str(self.torrent_queue[0]))}')
            if len(self.torrent_buffer) == 0:
                return
            self.torrent_queue.extend(copy(self.torrent_buffer))
            self.torrent_buffer.clear()
        if not self.t_active:
            threading.Thread(target=self._do_flush_torrents).start()

    def _do_flush_torrents(self):
        self.t_active = True
        conn = self.get_connection()
        while len(self.torrent_queue) > 0:
            cursor = conn.cursor()
            cursor.executemany('INSERT INTO torrents (ID, Seeders, Leechers, Snatched, Balance) '
                               'VALUES (%s, %s, %s, %s, %s) '
                               'ON DUPLICATE KEY UPDATE Seeders=VALUES(Seeders), '
                               'Leechers=VALUES(Leechers), '
                               'Snatched = Snatched + VALUES(Snatched), '
                               'Balance=VALUES(Balance), '
                               'last_action=IF(VALUES(Seeders) > 0, NOW(), last_action)',
                               self.torrent_queue[0])
            cursor.execute("DELETE FROM torrents WHERE info_hash = ''")
            cursor.close()
            with self.torrent_lock:
                self.torrent_queue.pop(0)
        self.t_active = False

    def _flush_snatches(self):
        if self.readonly:
            self.snatch_buffer.clear()
            return

        with self.snatch_lock:
            if len(self.snatch_queue) > 0:
                self.logger.info(f'Snatch flush queue size: {len(self.snatch_queue)}, '
                                 f'next query length: {len(str(self.snatch_queue[0]))}')
            if len(self.snatch_buffer) == 0:
                return
            self.snatch_queue.extend(copy(self.snatch_buffer))
            self.snatch_buffer.clear()
        if not self.s_active:
            threading.Thread(target=self._do_flush_snatches).start()

    def _do_flush_snatches(self):
        self.s_active = True
        conn = self.get_connection()
        while len(self.snatch_queue) > 0:
            cursor = conn.cursor()
            cursor.executemany('INSERT INTO xbt_snatched (uid, fid, tstamp, IP) '
                               'VALUES (%s, %s, %s, %s)', self.snatch_queue[0])

            cursor.close()
            with self.snatch_lock:
                self.snatch_queue.pop(0)
        self.s_active = False

    def _flush_peers(self):
        if self.readonly:
            self.heavy_peer_buffer.clear()
            self.light_peer_buffer.clear()
            return

        with self.peer_lock:
            if len(self.peer_queue) > 0:
                self.logger.info(f'Heavy peer queue size: {len(self.peer_queue)}, '
                                 f'next query length: {len(str(self.peer_queue[0]))}')
            if len(self.heavy_peer_buffer) > 0:
                if len(self.peer_queue) > 1000:
                    self.peer_queue.pop(0)
                self.peer_queue.extend(copy(self.heavy_peer_buffer))
                self.heavy_peer_buffer.clear()
            if len(self.light_peer_buffer) > 0:
                if len(self.peer_queue) > 1000:
                    self.peer_queue.pop(0)
                self.peer_queue.extend(copy(self.light_peer_buffer))
                self.light_peer_buffer.clear()
        if not self.p_active:
            threading.Thread(target=self._do_flush_peers).start()

    def _do_flush_peers(self):
        self.p_active = True
        conn = self.get_connection()

        while len(self.peer_queue) > 0:
            cursor = conn.cursor()
            if len(self.peer_queue[0]) == 4:
                cursor.executemany('INSERT INTO xbt_files_users (uid, fid, timespent, '
                                   'announced, peer_id, mtime) '
                                   'VALUES (%s, %s, %s, %s, %s, %s) '
                                   'ON DUPLICATE KEY UPDATE upspeed=0, downspeed=0, '
                                   'timespent=VALUES(timespent), announced=VALUES(announced), '
                                   'mtime=VALUES(mtime)', self.peer_queue[0])
            else:
                cursor.executemany('INSERT INTO xbt_files_users (uid, fid, active, uploaded, '
                                   'downloaded, upspeed, downspeed, remaining, corrupt, '
                                   'timespent, announced, ip, peer_id, useragent, mtime) '
                                   'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,'
                                   ' %s, %s) ON DUPLICATE KEY UPDATE active=VALUES(active), '
                                   'uploaded=VALUES(uploaded), downloaded=VALUES(downloaded), '
                                   'upspeed=VALUES(upspeed), downspeed=VALUES(downspeed), '
                                   'remaining=VALUES(remaining), corrupt=VALUES(corrupt), '
                                   'timespent=VALUES(timespent), announced=VALUES(announced), '
                                   'mtime=VALUES(mtime)', self.peer_queue[0])
            cursor.close()
            with self.peer_lock:
                self.peer_queue.pop(0)
        self.p_active = False

    def _flush_tokens(self):
        if self.readonly:
            self.token_buffer.clear()
            return

        with self.token_lock:
            if len(self.token_queue) > 0:
                self.logger.info(f'Token flush queue size: {len(self.token_queue)}, '
                                 f'next query length: {len(str(self.token_queue[0]))}')

            if len(self.token_buffer) == 0:
                return
            self.token_queue.extend(copy(self.token_buffer))
            self.token_buffer.clear()
            if not self.tok_active:
                threading.Thread(target=self._do_flush_tokens).start()

    def _do_flush_tokens(self):
        self.tok_active = True
        conn = self.get_connection()
        while len(self.token_queue) > 0:
            cursor = conn.cursor()
            cursor.executemany('INSERT INTO users_freeleeches (UserID, TorrentID, Downloaded) '
                               'VALUES(%s, %s, %s)', self.token_queue[0])
            cursor.close()
            with self.token_lock:
                self.token_queue.pop(0)
        self.tok_active = False

    def _clear_peer_data(self):
        self.db.query('TRUNCATE xbt_files_users')
        self.db.query('UPDATE torrents SET Seeders = 0, Leechers = 0')
