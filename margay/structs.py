from collections import OrderedDict
from enum import IntEnum


class User(object):
    def __init__(self, uid: int, leech: bool, protect: bool):
        self.id = uid
        self.leech = leech
        self.protect = protect
        self.leeching = 0
        self.seeding = 0
        self.deleted = False


class Peer(object):
    def __init__(self):
        self.uploaded = 0
        self.downloaded = 0
        self.corrupt = 0
        self.left = 0
        self.last_announced = None
        self.first_announced = None
        self.announces = 0
        self.port = None
        self.visible = False
        self.invalid_ip = False
        self.user = None  # type: User
        self.ip = None
        self.ip_port = ''
        self.port = None


class Torrent(object):
    def __init__(self, tid, completed):
        self.id = tid
        self.completed = completed
        self.balance = 0
        self.free_torrent = None
        self.last_flushed = 0
        self.seeders = OrderedDict()  # type: OrderedDict[str, Peer]
        self.leechers = OrderedDict()  # type: OrderedDict[str, Peer]
        self.last_selected_seeder = ''
        self.tokened_users = []


class ErrorCodes(IntEnum):
    DUPE = 0
    TRUMP = 1
    BAD_FILE_NAMES = 2
    BAD_FOLDER_NAMES = 3
    BAD_TAGS = 4
    BAD_FORMAT = 5
    DISCS_MISSING = 6
    DISCOGRAPHY = 7
    EDITED_LOG = 8
    INACCURATE_BITRATE = 9
    LOW_BITRATE = 10
    MUTT_RIP = 11
    BAD_SOURCE = 12
    ENCODE_ERRORS = 13
    BANNED = 14
    TRACKS_MISSING = 15
    TRANSCODE = 16
    CASSETTE = 17
    UNSPLIT_ALBUM = 18
    USER_COMPILATION = 19
    WRONG_FORMAT = 20
    WRONG_MEDIA = 21
    AUDIENCE = 22

    @classmethod
    def get_del_reason(cls, value):
        if value == cls.DUPE:
            return 'Dupe'
        elif value == cls.TRUMP:
            return 'Trump'
        elif value == cls.BAD_FILE_NAMES:
            return 'Bad File Names'
        elif value == cls.BAD_FOLDER_NAMES:
            return 'Bad Folder Names'
        elif value == cls.BAD_TAGS:
            return 'Bad Tags'
        elif value == cls.BAD_FORMAT:
            return 'Disallowed Format'
        elif value == cls.DISCS_MISSING:
            return 'Discs Missing'
        elif value == cls.DISCOGRAPHY:
            return 'Discography'
        elif value == cls.EDITED_LOG:
            return 'Edited Log'
        elif value == cls.INACCURATE_BITRATE:
            return 'Inaccurate Bitrate'
        elif value == cls.LOW_BITRATE:
            return 'Low Bitrate'
        elif value == cls.MUTT_RIP:
            return 'Mutt Rip'
        elif value == cls.BAD_SOURCE:
            return 'Disallowed Source'
        elif value == cls.ENCODE_ERRORS:
            return 'Encode Errors'
        elif value == cls.BANNED:
            return 'Specifically Banned'
        elif value == cls.TRACKS_MISSING:
            return 'Tracks Missing'
        elif value == cls.TRANSCODE:
            return 'Transcode'
        elif value == cls.CASSETTE:
            return 'Unapproved Cassette'
        elif value == cls.UNSPLIT_ALBUM:
            return 'Unsplit Album'
        elif value == cls.USER_COMPILATION:
            return 'User Compilation'
        elif value == cls.WRONG_FORMAT:
            return 'Wrong Format'
        elif value == cls.WRONG_MEDIA:
            return 'Wrong Media'
        elif value == cls.AUDIENCE:
            return 'Audience Recording'
        else:
            return ''


class LeechType(IntEnum):
    NORMAL = 0
    FREE = 1
    NEUTRAL = 2

    @classmethod
    def to_enum(cls, value):
        for enum in cls:
            if value == str(enum.value):
                return enum
        raise ValueError('Invalid leech type')


torrent_list = dict()
user_list = dict()
peer_list = dict()
