import ipaddress
from enum import Enum, auto
import logging
import re
from time import time
import threading
from typing import Dict, List

# noinspection PyPackageRequirements
import bencode
from aiohttp import web

from .structs import ErrorCodes, LeechType, Peer, Torrent, User
import margay.stats as stats

REGEX = re.compile(r'info_hash=([%a-zA-Z0-9]+)')


class Status(Enum):
    OPEN = auto()
    PAUSED = auto()
    CLOSING = auto()


class Worker(object):
    def __init__(self, database, site_comm, config):
        self.logger = logging.getLogger()
        self.database = database
        self.site_comm = site_comm
        self.config = config
        self.torrents = dict()  # type: Dict[str, Torrent]
        self.users = dict()  # type: Dict[str, User]
        self.whitelist = list()  # type: List[str]

        self.del_reasons = dict()
        self.del_reasons_lock = threading.RLock()

        self.announce_interval = 0
        self.del_reason_lifetime = 0
        self.peers_timeout = 0
        self.numwant_limit = 0
        self.site_password = ''
        self.report_password = ''

        self.status = Status.OPEN

        self.reaper_active = False

        self.load_config(self.config)
        self.reload_lists()

    def load_config(self, config):
        self.announce_interval = config['tracker']['announce_interval']
        self.del_reason_lifetime = config['timers']['del_reason_lifetime']
        self.peers_timeout = config['timers']['peers_timeout']
        self.numwant_limit = config['tracker']['numwant_limit']
        self.site_password = config['gazelle']['site_password']
        self.report_password = config['gazelle']['report_password']

    def reload_config(self, config):
        self.load_config(config)

    def shutdown(self):
        if self.status == Status.OPEN:
            self.status = Status.CLOSING
            self.logger.info('closing tracker... press Ctrl+C again to terminate')
            return False
        elif self.status == Status.CLOSING:
            self.logger.info('shutting down uncleanly')
            return True
        else:
            return False

    def reload_lists(self):
        self.status = Status.PAUSED
        self.torrents = self.database.load_torrents(self.torrents)
        self.users = self.database.load_users(self.users)
        self.whitelist = self.database.load_whitelist()
        self.status = Status.OPEN

    def create_server(self, port):
        app = web.Application()
        app.router.add_get('/', self.handler_null)
        app.router.add_get('/{passkey}/{action}', self.handler_work)
        self.logger.info(f'======== Running on http://127.0.0.1:{port} ========')
        web.run_app(app, host='127.0.0.1', print=False, port=port, handle_signals=False)

    async def handler_null(self):
        return self.handle_null()

    # noinspection PyMethodMayBeStatic
    def handle_null(self):
        return web.Response(text='Nothing to see here.')

    def error(self, message):
        response = {'failure reason': message, 'min interval': 5400, 'interval': 5400}
        return self.response(response)

    def warning(self, message):
        return self.response({'warning message': message})

    async def handler_work(self, request):
        action = request.match_info.get('action').lower()
        actions = ['announce', 'scrape', 'update', 'report']
        if action not in actions:
            return web.Response(text='Invalid action.')
        if len(request.query) == 0:
            return self.handle_null()

        if self.status != Status.OPEN:
            return self.error('The tracker is temporarily unavailable.')

        passkey = request.match_info.get('passkey')
        if action == 'update' or action == 'report':
            if passkey != self.site_password:
                return self.error('Authentication failure.')

        if action == 'update':
            return self.handle_update(request)
        elif action == 'report':
            return self.handle_report(request)

        with self.database.user_list_lock:
            if passkey not in self.users:
                return self.error('Passkey not found')
            user = self.users[passkey]

        if action == 'announce':
            return self.handle_announce(request, user)
        elif action == 'scrape':
            return self.handle_scrape(request)

    def handle_announce(self, request, user):
        params = request.query
        with self.database.torrent_list_lock:
            tor = self.torrents[params['info_hash']]  # type: Torrent
        cur_time = int(time())
        if params['compact'] != '1':
            return self.error('Your client does not support compact announces')

        left = max(0, int(params['left']))
        uploaded = max(0, int(params['uploaded']))
        downloaded = max(0, int(params['downloaded']))
        corrupt = max(0, int(params['corrupt']))

        snatched = 0
        active = 1
        inserted = False
        update_torrent = False
        completed_torrent = False
        stopped_torrent = False
        expire_token = False
        peer_changed = False
        invalid_ip = False
        inc_l = inc_s = dec_l = dec_s = False

        if 'peer_id' not in params:
            return self.error('No peer ID')
        elif len(params['peer_id']) != 20:
            return self.error('Invalid peer ID')

        with self.database.whitelist_lock:
            if len(self.whitelist) > 0:
                found = False
                for client in self.whitelist:
                    if params['peer_id'].startswith(client):
                        found = True

                if not found:
                    return self.error('Your client is not on the whitelist')

        peer_key = params['peer_id'][12 + (tor.id & 7)] + str(user.id) + params['peer_id']

        if params['event'] == 'completed':
            completed_torrent = left == 0
        elif params['event'] == 'stopped':
            stopped_torrent = True
            peer_changed = True
            update_torrent = True
            active = 0

        peer = None  # type: Peer
        if left > 0:
            if peer_key not in tor.leechers:
                peer = Peer()
                tor.leechers[peer_key] = peer
                inserted = True
                inc_l = True
            else:
                peer = tor.leechers[peer_key]
        elif completed_torrent:
            if peer_key not in tor.leechers:
                if peer_key not in tor.seeders:
                    peer = Peer()
                    tor.seeders[peer_key] = peer
                    inserted = True
                    inc_s = True
                else:
                    peer = tor.seeders[peer_key]
                    completed_torrent = False
            elif peer_key in tor.seeders:
                peer = tor.leechers[peer_key]
                dec_s = True
        else:
            if peer_key not in tor.seeders:
                if peer_key not in tor.leechers:
                    peer = Peer()
                    tor.seeders[peer_key] = peer
                    inserted = True
                else:
                    peer = tor.leechers[peer_key]
                    tor.seeders[peer_key] = peer
                    del tor.leechers[peer_key]
                    peer_changed = True
                    dec_l = True

        upspeed = 0
        downspeed = 0

        if inserted or params['event'] == 'started':
            update_torrent = True
            if inserted:
                peer.user = user
            peer.first_announced = cur_time
            peer.last_announced = 0
            peer.uploaded = uploaded
            peer.downloaded = downloaded
            peer.corrupt = corrupt
            peer.announces = 1
            peer_changed = True
        elif uploaded < peer.uploaded or downloaded < peer.downloaded:
            peer.announces += 1
            peer.uploaded = uploaded
            peer.downloaded = downloaded
            peer_changed = True
        else:
            uploaded_change = 0
            downloaded_change = 0
            corrupt_change = 0
            peer.announces += 1

            if uploaded != peer.uploaded:
                uploaded_change = uploaded - peer.uploaded
                peer.uploaded = uploaded
            if downloaded != peer.downloaded:
                downloaded_change = downloaded - peer.downloaded
                peer.downloaded = downloaded
            if corrupt != peer.corrupt:
                corrupt_change = corrupt - peer.corrupt
                peer.corrupt = corrupt
                tor.balance -= corrupt_change
                update_torrent = True
            peer_changed = peer_changed or uploaded_change or downloaded_change or corrupt_change

            if uploaded_change or downloaded_change:
                tor.balance += uploaded_change
                tor.balance -= downloaded_change
                update_torrent = True

                if cur_time > peer.last_announced:
                    upspeed = uploaded_change / (cur_time - peer.last_announced)
                    downspeed = downloaded_change / (cur_time - peer.last_announced)

                tokened = user.id in tor.tokened_users
                if tor.free_torrent == LeechType.NEUTRAL:
                    downloaded_change = 0
                    uploaded_change = 0
                elif tor.free_torrent == LeechType.FREE or tokened:
                    if tokened:
                        expire_token = True
                        self.database.record_token(user.id, tor.id, downloaded_change)
                    downloaded_change = 0

                if uploaded_change or downloaded_change:
                    self.database.record_user(user.id, uploaded_change, downloaded_change)

        peer.left = left

        if 'ip' in params:
            ip = params['ip']
        elif 'ipv4' in params:
            ip = params['ipv4']
        else:
            ip = request.headers['x-forwarded-for'].split(',')[0]

        port = int(params['port'])

        if inserted or port != peer.port or ip != peer.ip:
            peer.ip = ip
            peer.port = port
            parsed = ipaddress.ip_address(ip)
            if parsed.is_private or parsed.is_unspecified or parsed.is_reserved or parsed.is_loopback:
                invalid_ip = True
            if not invalid_ip:
                peer.ip_port = parsed.packed + port.to_bytes(length=2, byteorder='big')
            if len(peer.ip_port) != 6:
                peer.ip_port = ''
                invalid_ip = True
            peer.invalid_ip = invalid_ip
        else:
            invalid_ip = peer.invalid_ip

        peer.last_announced = cur_time

        # Peer is visible in the lists if they have their leech priviledges and they're not
        # using an invalid IP address
        peer.visible = (peer.left == 0 or user.leech) and not peer.invalid_ip

        if peer_changed:
            record_ip = '' if user.protect else ip
            self.database.record_peer_heavy(user.id, tor.id, active, uploaded, downloaded,
                                            upspeed, downspeed, left, corrupt,
                                            (cur_time - peer.first_announced), peer.announces,
                                            record_ip, params['peer_id'],
                                            request.headers['user-agent'])
        else:
            self.database.record_peer_light(user.id, tor.id, (cur_time - peer.first_announced),
                                            peer.announces, params['peer_id'])

        numwant = self.numwant_limit
        if 'numwant' in params:
            numwant = min(params['numwant'], numwant)

        if stopped_torrent:
            numwant = 0
            if left > 0:
                dec_l = True
            else:
                dec_s = True
        elif completed_torrent:
            snatched = 1
            update_torrent = True
            tor.completed += 1

            record_ip = '' if user.protect else ip
            self.database.record_snatch(user.id, tor.id, cur_time, record_ip)

            if not inserted:
                tor.seeders[peer_key] = peer
                del tor.leechers[peer_key]
                dec_l = inc_s = True

            if expire_token:
                self.site_comm.expire_token(tor.id, user.id)
                tor.tokened_users.remove(user.id)
        elif not user.leech and left > 0:
            numwant = 0

        peers = b''
        if numwant > 0:
            found_peers = 0
            if left > 0:
                if len(tor.seeders) > 0:
                    seeders_list = list(tor.seeders.keys())
                    i = 0

                    # Find out where to begin in the seeder list
                    if tor.last_selected_seeder != '':
                        try:
                            i = seeders_list.index(tor.last_selected_seeder)
                            i += 1
                            if i == len(seeders_list):
                                i = 0
                        except ValueError:
                            pass

                    # Find out where to end in the seeder list
                    end = len(seeders_list)
                    if i != 0:
                        end = i - 1
                        if end == 0:
                            end += 1
                            i += 1

                    while i != end and found_peers < numwant:
                        if i == len(seeders_list):
                            i = 0
                        seeder = tor.seeders[seeders_list[i]]
                        # Don't show users to themselves or leech disabled users
                        if seeder.user.deleted or seeder.user.id == user.id or not seeder.visible:
                            i += 1
                            continue
                        found_peers += 1
                        peers += seeder.ip_port

                if found_peers < numwant and len(tor.leechers) > 1:
                    for key in tor.leechers:
                        leecher = tor.leechers[key]
                        if leecher.user.deleted or leecher.ip_port == peer.ip_port or leecher.user.id == user.id or not leecher.visible:
                            continue
                        found_peers += 1
                        peers += leecher.ip_port
                        if found_peers >= numwant:
                            break
            elif len(tor.leechers) > 0:
                for key in tor.leechers:
                    leecher = tor.leechers[key]
                    if leecher.user.id == user.id or not leecher.visible:
                        continue
                    found_peers += 1
                    peers += leecher.ip_port
                    if found_peers >= numwant:
                        break


        stats.succ_announcements += 1
        if dec_l or dec_s or inc_l or inc_s:
            if inc_l:
                peer.user.leeching += 1
                stats.leechers += 1
            if inc_s:
                peer.user.seeding += 1
                stats.seeders += 1
            if dec_l:
                peer.user.leeching -= 1
                stats.leechers -= 1
            if dec_s:
                peer.user.seeding -= 1
                stats.seeders -= 1

        if peer.user != user:
            if not stopped_torrent:
                if left > 0:
                    user.leeching += 1
                    peer.user.leeching -= 1
                else:
                    user.seeding += 1
                    peer.user.seeding -= 1
            peer.user = user

        if stopped_torrent:
            if left > 0:
                del tor.leechers[peer_key]
            else:
                del tor.seeders[peer_key]

        if update_torrent or tor.last_flushed + 3600 < cur_time:
            tor.last_flushed = cur_time

            self.database.record_torrent(tor.id, len(tor.seeders), len(tor.leechers), snatched,
                                         tor.balance)

        if not user.leech and left > 0:
            return self.error('Access denied, leeching forbidden')

        response = {
            'complete': len(tor.seeders),
            'downloaded': tor.completed,
            'incomplete': len(tor.leechers),
            'interval': self.announce_interval + min(600, len(tor.seeders)),  # ensure a more even distribution of announces/second
            'min interval': self.announce_interval,
            'peers': peers
        }

        if invalid_ip:
            response['warning message'] = 'Illegal character found in IP address. IPv6 is not ' \
                                          'supported'

        return web.Response(text=bencode.encode(response))

    def handle_scrape(self, request):
        response = {'files': {}}
        for infohash in request.query['info_hash']:
            if infohash not in self.torrents:
                continue
            t = self.torrents[infohash]
            response['files'][infohash] = {
                'complete': len(t.seeders),
                'incomplete': len(t.leechers),
                'downloaded': t.completed
            }
        return web.Response(text=bencode.encode(response))

    def handle_update(self, request):
        params = request.query
        if params['action'] == 'change_passkey':
            oldpasskey = params['oldpasskey']
            newpasskey = params['newpasskey']
            with self.database.user_list_lock:
                if oldpasskey not in self.users:
                    self.logger.warning(f'No user with passkey {oldpasskey} exists when '
                                        f'attempting to change passkey to {newpasskey}')
                else:
                    self.users[newpasskey] = self.users[oldpasskey]
                    del self.users[oldpasskey]
                    self.logger.info(f'Changed passkey from {oldpasskey} to {newpasskey} for '
                                     f'user {self.users[newpasskey].id}')
        elif params['action'] == 'add_torrent':
            info_hash = params['info_hash']
            with self.database.torrent_list_lock:
                if info_hash not in self.torrents:
                    torrent = Torrent(params['id'], 0)
                else:
                    torrent = self.torrents[info_hash]
                if params['freetorrent'] == '0':
                    torrent.free_torrent = LeechType.NORMAL
                elif params['freetorrent'] == '1':
                    torrent.free_torrent = LeechType.FREE
                else:
                    torrent.free_torrent = LeechType.NEUTRAL
                self.torrents[info_hash] = torrent
                self.logger.info(f"Added torrent {torrent.id}. FL: {torrent.free_torrent} "
                                 f"{params['freetorrent']}")
        elif params['action'] == 'update_torrent':
            info_hash = params['info_hash']
            if params['freetorrent'] == '0':
                fl = LeechType.NORMAL
            elif params['freetorrent'] == '1':
                fl = LeechType.FREE
            else:
                fl = LeechType.NEUTRAL
            with self.database.torrent_list_lock:
                if info_hash in self.torrents:
                    self.torrents[info_hash].free_torrent = fl
                    self.logger.info(f'Updated torrent {self.torrents[info_hash].id} to FL {fl}')
                else:
                    self.logger.warning(f'Failed to find torrent {info_hash} to FL {fl}')
        elif params['action'] == 'update_torrents':
            # Each decoded infohash is exactly 20 characters long
            # TODO: this probably doesn't work and needs more work
            info_hashes = params['info_hashes']
            if params['freetorrent'] == '0':
                fl = LeechType.NORMAL
            elif params['freetorrent'] == '1':
                fl = LeechType.FREE
            else:
                fl = LeechType.NEUTRAL
            with self.database.torrent_list_lock:
                pos = 0
                while pos < len(info_hashes):
                    info_hash = info_hashes[pos:pos+20]
                    if info_hash in self.torrents:
                        self.torrents[info_hash].free_torrent = fl
                        self.logger.info(f'Updated torrent {self.torrents[info_hash].id} '
                                         f'to FL {fl}')
                    else:
                        self.logger.warning(f'Failed to find torrent {info_hash} to FL {fl}')
        elif params['action'] == 'add_token':
            info_hash = params['info_hash']
            userid = int(params['userid'])
            with self.database.torrent_list_lock:
                if info_hash in self.torrents:
                    self.torrents[info_hash].tokened_users.remove(userid)
                else:
                    self.logger.warning(f'Failed to find torrent to add a token for user {userid}')
        elif params['action'] == 'remove_token':
            info_hash = params['info_hash']
            userid = int(params['userid'])
            with self.database.torrent_list_lock:
                if info_hash in self.torrents:
                    self.torrents[info_hash].tokened_users.remove(userid)
                else:
                    self.logger.warning(f'Failed to find torrent {info_hash} to remove token '
                                        f'for user {userid}')
        elif params['action'] == 'delete_torrent':
            info_hash = params['info_hash']
            reason = int(params['reason']) if 'reason' in params else -1
            with self.database.torrent_list_lock:
                if info_hash in self.torrents:
                    torrent = self.torrents[info_hash]
                    self.logger.info(f'Deleting torrent {torrent.id} for the '
                                     f'reason {ErrorCodes.get_del_reason(reason)}')
                    stats.leechers -= len(torrent.leechers)
                    stats.seeders -= len(torrent.seeders)
                    for peer_key in torrent.leechers:
                        torrent.leechers[peer_key].user.leeching -= 1
                    for peer_key in torrent.seeders:
                        torrent.seeders[peer_key].user.seeding -= 1
                    with self.del_reasons_lock:
                        self.del_reasons[info_hash] = {'reason': reason, time: int(time())}
                        del self.torrents[info_hash]
                else:
                    self.logger.warning(f'Failed to find torrent {info_hash} to delete')
        elif params['action'] == 'add_user':
            passkey = params['passkey']
            userid = int(params['id'])
            with self.database.user_list_lock:
                if passkey not in self.users:
                    self.users[passkey] = User(userid, True, params['visible'] == '0')
                    self.logger.info(f'Added user {passkey} with id {userid}')
                else:
                    self.logger.warning(f'Tried to add already known user {passkey} '
                                        f'with id {self.users[passkey].id}')
                    self.users[passkey].deleted = True
        elif params['action'] == 'remove_user':
            passkey = params['passkey']
            with self.database.user_list_lock:
                if passkey in self.users:
                    self.logger.info(f'Removed user {passkey} with id {self.users[passkey].id}')
                    self.users[passkey].deleted = True
                    del self.users[passkey]
        elif params['action'] == 'remove_users':
            # Each passkey is 32 characters long
            passkeys = params['passkeys']
            with self.database.user_list_lock:
                i = 0
                while i < len(passkeys):
                    passkey = passkeys[i:i+32]
                    if passkey in self.users:
                        self.logger.info(f'Removed user {passkey}')
                        self.users[passkey].deleted = True
                        del self.users[passkey]
                    i += 32
        elif params['action'] == 'update_user':
            passkey = params['passkey']
            can_leech = False if params['can_leech'] == '0' else True
            protect_ip = True if params['visible'] == '0' else False
            with self.database.user_list_lock:
                if passkey not in self.users:
                    self.logger.warning(f'No user with passkey {passkey} found when attempting to '
                                        f'change leeching status!')
                else:
                    self.users[passkey].protect = protect_ip
                    self.users[passkey].leech = can_leech
                    self.logger.info(f'Updated user {passkey}')
        elif params['action'] == 'add_whitelist':
            peer_id = params['peer_id']
            with self.database.whitelist_lock:
                self.whitelist.append(peer_id)
                self.logger.info(f'Whitelisted {peer_id}')
        elif params['action'] == 'remove_whitelist':
            peer_id = params['peer_id']
            with self.database.whitelist_lock:
                try:
                    self.whitelist.remove(peer_id)
                except ValueError:
                    pass
                self.logger.info(f'De-whitelisted {peer_id}')
        elif params['action'] == 'edit_whitelist':
            new_peer_id = params['new_peer_id']
            old_peer_id = params['old_peer_id']
            with self.database.whitelist_lock:
                try:
                    self.whitelist.remove(old_peer_id)
                except ValueError:
                    pass
                self.whitelist.append(new_peer_id)
                self.logger.info(f'Edited whitelist item from {old_peer_id} to {new_peer_id}')
        elif params['action'] == 'update_announce_interval':
            self.announce_interval = int(params['announce_interval'])
            self.config['tracker']['announce_interval'] = self.announce_interval
            self.logger.info(f'Edited announce interval to {self.announce_interval}')
        elif params['action'] == 'info_torrent':
            info_hash = params['info_hash']
            self.logger.info(f"Info for torrent '{info_hash}'")
            with self.database.torrent_list_lock:
                if info_hash in self.torrents:
                    self.logger.info(f'Torrent {self.torrents[info_hash].id}, '
                                     f'freetorrent = {self.torrents[info_hash].free_torrent}')
                else:
                    self.logger.warning(f'Failed to find torrent {info_hash}')
                    
        return web.Response(text='success')

    def handle_report(self, request):
        params = request.query
        action = params['get']
        output = ''
        if action == '':
            output += "Invalid action\n"
        elif action == 'stats':
            uptime = int(time()) - stats.start_time
            up_d = uptime // 86400
            uptime -= up_d * 86400
            up_h = uptime // 3600
            uptime -= up_h * 3600
            up_m = uptime // 60
            up_s = uptime - up_m * 60
            output += f"Uptime {up_d} days, {up_h:02}:{up_m:02}:{up_s:02}\n" \
                      f"{stats.opened_connections} connections opened\n" \
                      f"{stats.open_connections} open connections\n" \
                      f"{stats.connection_rate} connections/s\n" \
                      f"{stats.requests} requests handled\n" \
                      f"{stats.request_rate} requests/s\n" \
                      f"{stats.succ_announcements} successful announcements\n" \
                      f"{(stats.announcements - stats.succ_announcements)} failed announcements\n" \
                      f"{stats.scrapes} scrapes\n" \
                      f"{stats.leechers} leechers tracked\n" \
                      f"{stats.seeders} seeders tracked\n" \
                      f"{stats.bytes_read} bytes read\n" \
                      f"{stats.bytes_written} bytes written\n"
        elif action == 'user':
            key = params['key']
            if len(key) == 0:
                output += "Invalid action\n"
            else:
                with self.database.user_list_lock:
                    if key in self.users:
                        output += f"{self.users[key].leeching} leeching\n" \
                                  f"{self.users[key].seeding} seeding\n"
        else:
            output += "Invalid action\n"
        return web.Response(text=output)

    # noinspection PyMethodMayBeStatic
    def response(self, response):
        return web.Response(text=bencode.bencode(response))

    def start_reaper(self):
        if not self.reaper_active:
            threading.Thread(target=self._do_start_reaper)

    def _do_start_reaper(self):
        self.reaper_active = True
        self.reap_peers()
        self.reap_del_reasons()
        self.reaper_active = False

    def reap_peers(self):
        self.logger.info('Starting peer reaper')
        cur_time = int(time())
        reaped_l = reaped_s = 0
        cleared_torrents = 0
        for info_hash in self.torrents:
            reaped_this = False
            torrent = self.torrents[info_hash]
            for peer_key in torrent.leechers:
                if torrent.leechers[peer_key].last_announced + self.peers_timeout < cur_time:
                    with self.database.torrent_list_lock:
                        torrent.leechers[peer_key].user.leeching -= 1
                        del torrent.leechers[peer_key]
                        reaped_this = True
                        reaped_l += 1
            for peer_key in torrent.seeders:
                if torrent.seeders[peer_key].last_announced + self.peers_timeout < cur_time:
                    with self.database.torrent_list_lock:
                        torrent.seeders[peer_key].user.seeding -= 1
                        del torrent.seeders[peer_key]
                        reaped_this = True
                        reaped_s += 1
            if reaped_this and len(torrent.seeders) == 0 and len(torrent.leechers) == 0:
                self.database.record_torrent(torrent.id, 0, 0, 0, torrent.balance)
                cleared_torrents += 1
        if reaped_l > 0 or reaped_s > 0:
            stats.leechers -= reaped_l
            stats.seeders -= reaped_s
        self.logger.info(f'Reaped {reaped_l} leechers and {reaped_s} seeders. '
                         f'Reset {cleared_torrents} torrents')

    def reap_del_reasons(self):
        self.logger.info('Starting del reason reaper')
        max_time = int(time()) - self.del_reason_lifetime
        reaped = 0
        for key in self.del_reasons:
            if self.del_reasons[key]['time'] <= max_time:
                with self.del_reasons_lock:
                    del self.del_reasons[key]
                    reaped += 1

        self.logger.info(f'Reaped {reaped} del reasons')
