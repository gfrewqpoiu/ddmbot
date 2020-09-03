import functools
import logging
import re
from asyncio import AbstractEventLoop

import peewee
import youtube_dl
from playhouse.pool import PostgresqlExtDatabase
from typing import Any, Callable, Optional, Union

# set up the logger
log = logging.getLogger('ddmbot.database')
peewee = peewee

# database object
# TODO: This doesn't work with Deferred Foreign Keys. Switch to PooledPostgres.
# _database = peewee.SqliteDatabase(None, pragmas=[('journal_mode', 'WAL'), ('foreign_keys', 'ON')])
_database = PostgresqlExtDatabase(None)


class DdmBotSchema(peewee.Model):
    class Meta:
        database = _database


# Class to store timestamp in the database
class CreditTimestamp(DdmBotSchema):
    last = peewee.DateTimeField()


# Class representing a song table in the database
class Song(DdmBotSchema):
    # we use integer primary keys to represent songs in the database
    id = peewee.PrimaryKeyField()

    # song unique URI for consistent lookup and search
    uuri = peewee.CharField(index=True, unique=True)
    # title can be changed eventually
    title = peewee.CharField()

    # constrains
    duration = peewee.IntegerField()
    is_blacklisted = peewee.BooleanField(default=False)

    # overplaying protection
    last_played = peewee.DateTimeField()
    credit_count = peewee.IntegerField()

    # automatic playlist
    listener_count = peewee.IntegerField(default=0)
    skip_vote_count = peewee.IntegerField(default=0)
    has_failed = peewee.BooleanField(default=False)

    # song may be duplicated using multiple sources
    duplicate = peewee.ForeignKeyField('self', null=True)

#  TODO: This no longer works in Peewee 3.0.
# we will need this to resolve a foreign key loop
# DeferredUser = peewee.DeferredRelation()
# DeferredLink = peewee.DeferredRelation()


# Table for storing playlists, as many as user wants
class Playlist(DdmBotSchema):
    id = peewee.PrimaryKeyField()

    # playlist is owned by a user
    user = peewee.DeferredForeignKey('User', null=False)
    # for an identifier, we choose a "nice enough" name
    name = peewee.CharField()
    # the first song of the playlist
    head = peewee.DeferredForeignKey('Link', null=True, default=None)
    # playlist may be set to repeat itself, this is default except to implicit one
    repeat = peewee.BooleanField(default=True)

    class Meta:
        # we want the couple (user, name) to be unique (so no user has two playlists with the same name)
        constraints = [peewee.SQL('UNIQUE(user_id, name)')]


# Table for storing songs in playlist -- linked list approach
class Link(DdmBotSchema):
    id = peewee.PrimaryKeyField()

    playlist = peewee.ForeignKeyField(Playlist)
    song = peewee.ForeignKeyField(Song)
    next = peewee.ForeignKeyField('self', null=True)


# Finally, table for storing information about users
class User(DdmBotSchema):
    # we will re-use discord snowflakes (64-bit integers) as primary keys
    id = peewee.BigIntegerField(primary_key=True)

    # not everyone has to have a playlist
    active_playlist = peewee.ForeignKeyField(Playlist, null=True, default=None)
    play_count = peewee.IntegerField(default=0)
    listen_count = peewee.IntegerField(default=0)

    # for checking if the user should be ignored by the ddmbot
    is_ignored = peewee.BooleanField(default=False)


# # Model to retrieve failed foreign key constrains
# class ForeignKeyCheckModel(DdmBotSchema):
#     table = peewee.CharField()
#     rowid = peewee.BigIntegerField()
#     parent = peewee.CharField()
#     fkid = peewee.IntegerField()


class DBInterface:
    def __init__(self, loop: AbstractEventLoop):
        if _database.is_closed():
            raise RuntimeError('Database must be initialized and opened before instantiating interfaces')
        self._loop = loop
        self._database = _database


# decorator for DBInterface methods
def in_executor(method: Callable[..., Any]) -> Any:
    """Runs the given method in it's own loop worker thread pool."""
    def wrapped_method(self, *args, **kwargs):
        func = functools.partial(method, self, *args, **kwargs)
        return self._loop.run_in_executor(None, func)

    return wrapped_method


class DBSongUtil:
    # some class (static) constant variables
    _yt_regex = re.compile(r'^(https?://)?(www\.)?youtu(\.be/|be.com/.+?[?&]v=)(?P<id>[a-zA-Z0-9_-]+)')
    _sc_regex = re.compile(r'^(https?://)?soundcloud.com/(?P<artist>[^/]+)/(?P<track>[^/?]+)')
    _bc_regex = re.compile(r'^(https?://)?(?P<artist>[^.]+).bandcamp.com/track/(?P<track>[^/?]+)')
    _list_regex = re.compile(
        r'^(https?://)?(www\.youtube\.com/.*[?&]list=.+|soundcloud\.com/[^/]+/sets/.+|[^.:/]+\.bandcamp.com/album/.+)$')
    _url_base = {'yt': 'https://www.youtube.com/watch?v={}',
                 'sc': 'https://soundcloud.com/{}/{}',
                 'bc': 'https://{}.bandcamp.com/track/{}'}

    _ytdl = youtube_dl.YoutubeDL({'extract_flat': 'in_playlist', 'format': 'bestaudio/best', 'quiet': True,
                                  'no_color': True})

    @staticmethod
    def _make_url(song_uuri):
        uuri_parts = song_uuri.split(':')
        return DBSongUtil._url_base[uuri_parts[0]].format(*uuri_parts[1:])

    @staticmethod
    def _is_list(input_url) -> bool:
        return DBSongUtil._list_regex.match(input_url) is not None

    @staticmethod
    def _make_uuri(song_url: str) -> Optional[str]:
        # makes unique URI from URLs suitable for database storage
        # method will return URI in one of the following formats:
        #   yt:<youtube_id> for youtube video
        #   sc:<artist>:<track> for soundcloud
        #   bc:<artist>:<track> for bandcamp
        match = DBSongUtil._yt_regex.match(song_url)
        if match:
            return 'yt:{}'.format(match.group('id'))
        match = DBSongUtil._sc_regex.match(song_url)
        if match:
            return 'sc:{}:{}'.format(match.group('artist'), match.group('track'))
        match = DBSongUtil._bc_regex.match(song_url)
        if match:
            return 'bc:{}:{}'.format(match.group('artist'), match.group('track'))
        return None


class DBPlaylistUtil:
    _playlist_regex = re.compile(r'^[a-zA-Z0-9_-]{1,32}$')

    @staticmethod
    def _get_playlist(user_id: int, playlist_name: str) -> Playlist:
        try:
            playlist = Playlist.select().where(Playlist.user == user_id, Playlist.name == playlist_name).get()
        except Playlist.DoesNotExist as e:
            raise KeyError('You don\'t have a playlist called {}'.format(playlist_name)) from e
        return playlist

    @staticmethod
    def _get_playlist_ex(user_id, *, playlist_name=None, create_default=False):
        created = False
        # if the name is given, we can get the playlist in a typical way
        if playlist_name is not None:
            return DBPlaylistUtil._get_playlist(user_id, playlist_name), created

        # else we're gonna try to get an active playlist and possibly create it
        with _database.atomic():
            try:
                playlist = Playlist.select(Playlist).join(User, on=(User.active_playlist == Playlist.id)) \
                    .where(User.id == user_id).get()
            except Playlist.DoesNotExist as e:
                if create_default and Playlist.select().where(Playlist.user == user_id).count() == 0:
                    playlist = Playlist.create(user=user_id, name='default', repeat=False)
                    User.update(active_playlist=playlist.id).where(User.id == user_id).execute()
                    created = True
                else:
                    raise LookupError('You don\'t have an active playlist') from e

            return playlist, created


#
# Function to initialize and open database connection to a given file
#
# Integrity check is performed.
#
def initialize(db_name: str) -> None:
    if not _database.is_closed():
        raise RuntimeError('Database is opened already')

    _database.init(db_name)
    _database.connect()
    _database.create_tables([CreditTimestamp, Song, Playlist, Link, User], safe=True)
    try:
        Playlist._schema.create_foreign_key(Playlist.user)
    except peewee.ProgrammingError:
        _database.close()
        _database.connect()

    try:
        Playlist._schema.create_foreign_key(Playlist.head)
    except peewee.ProgrammingError:
        _database.close()
        _database.connect()

    # No longer needed when switching to Postgresql.
    # # check for the failed foreign key constrains
    # failed_query = ForeignKeyCheckModel.raw('PRAGMA foreign_key_check;')
    # if len(failed_query.execute()):
    #     _database.close()
    #     raise RuntimeError('Foreign key constraints check failed, database is corrupted and needs to be fixed')


#
# Function taking care of properly closing database
#
def close() -> None:
    _database.close()
