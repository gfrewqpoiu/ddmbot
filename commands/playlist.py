import discord.ext.commands as dec

import database.playlist


class Playlist:
    """Playlist manipulation"""
    def __init__(self, bot):
        self._bot = bot
        self._db = database.playlist.PlaylistInterface(bot.loop, bot.config['ddmbot'])

    _help_messages = {
        'group': 'Playlist manipulation, switching, listing playlists and their content',

        'active': 'Displays the name of your active playlist\n\n',

        'create': 'Creates new playlist with a given name\n\n'
        'The default behaviour is to repeat the songs from the playlist in a loop.',

        'clear': 'Clears the specified playlist\n\n'
        'All songs from the given playlist will be removed. This cannot be undone.\nIf no playlist is specified, your '
        'active playlist is cleared.',

        'list': 'Lists the available playlists or songs in them\n\n'
        'When executed without arguments, list of your playlist is be returned. If you specify a playlist, list of '
        'songs from that playlist is returned.\n\nDue to message length restrictions, up to 20 songs are returned for '
        'a single request. By default, songs from the beginning of your playlist are listed. You can list the rest '
        'of your playlist by specifying the offset with another optional argument (e.g. command with offset 17 will '
        'list songs from the specified playlist at positions 17 to 36).',

        'peek': 'Lists the songs in your active playlist\n\n'
        'Quick way of listing your active playlist. Equivalent to \'playlist list <active_playlist_name> [start]\'.',

        'remove': 'Removes the specified playlist\n\n'
        'Playlist is removed along with all the songs in it. This cannot be undone.',

        'repeat': 'Set repeat behaviour for the specified playlist\n\n'
        'You can switch between removing and repeating songs from your active playlist after playing. The current '
        'setting can be queried with \'playlist list\' command, every playlist can be configured separately.\n'
        'When turned on, songs are simply reinserted at the end of the active playlist after being played.',

        'switch': 'Changes your active playlist\n\n'
        'Playlist specified will be set as your active playlist. Active playlist is the one used when playing songs '
        'from the DJ queue.',

        'shuffle': 'Shuffles songs in the specified playlist\n\n'
        'Randomly re-orders songs in the given playlist.\nIf no playlist is specified, your active playlist is '
        'shuffled.'
    }

    @dec.group(invoke_without_command=True, aliases=['p'], help=_help_messages['group'])
    async def playlist(self, subcommand: str, *arguments: str):
        raise dec.UserInputError('Command *playlist* has no subcommand named {}. Please use `{}help playlist` to '
                                 'list all the available subcommands.'
                                 .format(subcommand, self._bot.config['ddmbot']['delimiter']))

    @playlist.command(pass_context=True, ignore_extra=False, aliases=['a'], help=_help_messages['active'])
    async def active(self, ctx):
        name = await self._db.get_active(int(ctx.message.author.id))
        await self._bot.whisper('**Active playlist:** {}'.format(name))

    @playlist.command(pass_context=True, ignore_extra=False, help=_help_messages['create'])
    async def create(self, ctx, playlist_name: str):
        await self._db.create(int(ctx.message.author.id), playlist_name)
        await self._bot.whisper('**New playlist with the name** {} **was created**\nYour active playlist was switched '
                                ' to the newly created one.'.format(playlist_name))

    @playlist.command(pass_context=True, ignore_extra=False, help=_help_messages['clear'])
    async def clear(self, ctx, playlist_name: str = None):
        playlist_name = await self._db.clear(int(ctx.message.author.id), playlist_name)
        await self._bot.whisper('**Playlist** {} **was cleared**'.format(playlist_name))

    @playlist.command(pass_context=True, ignore_extra=False, aliases=['l'], help=_help_messages['list'])
    async def list(self, ctx, playlist_name: str = None, start: int = 1):
        # behaviour depends on the arguments, if playlist_name is given, list the songs from the playlist
        if playlist_name is not None:
            return await self._show(int(ctx.message.author.id), start=start, playlist_name=playlist_name)

        # else we need to list user's playlists
        items = await self._db.list(int(ctx.message.author.id))
        if not items:
            return await self._bot.whisper('**You don\'t have any playlists**')

        reply = '**You currently have {} playlist(s):**\n **>** '.format(len(items)) + \
                '\n **>** '.join(['{} ({} song(s), songs are {})'
                                 .format(item['name'], item['song_count'],
                                         'repeated' if item['repeat'] else 'removed after playing') for item in items])
        await self._bot.whisper(reply)

    @playlist.command(pass_context=True, ignore_extra=False, aliases=['p'], help=_help_messages['peek'])
    async def peek(self, ctx, start: int = 1):
        await self._show(int(ctx.message.author.id), start=start)

    @playlist.command(pass_context=True, ignore_extra=False, help=_help_messages['remove'])
    async def remove(self, ctx, playlist_name: str):
        await self._db.remove(int(ctx.message.author.id), playlist_name)
        await self._bot.whisper('**Playlist** {} **was removed**'.format(playlist_name))

    @playlist.command(pass_context=True, ignore_extra=False, help=_help_messages['repeat'])
    async def repeat(self, ctx, playlist_name: str, repeat_policy: str):
        if repeat_policy in ['on', 'true', '1', 'repeat']:
            setting = True
        elif repeat_policy in ['off', 'false', '0', 'remove']:
            setting = False
        else:
            await self._bot.whisper('Valid options are:\n    \'on\', \'true\', \'1\', \'repeat\'\nor\n    \'off\', '
                                    '\'false\', \'0\', \'remove\'\nrespectively')
            return

        await self._db.repeat(int(ctx.message.author.id), setting, playlist_name)
        message = '**Songs from the playlist** {} **will be '.format(playlist_name)
        if setting:
            message += "repeated after playing**"
        else:
            message += "removed after playing**"
        await self._bot.whisper(message)

    @playlist.command(pass_context=True, ignore_extra=False, aliases=['s'], help=_help_messages['switch'])
    async def switch(self, ctx, playlist_name: str):
        await self._db.set_active(int(ctx.message.author.id), playlist_name)
        await self._bot.whisper('**Playlist** {} **was set as active**'.format(playlist_name))

    @playlist.command(pass_context=True, ignore_extra=False, help=_help_messages['shuffle'])
    async def shuffle(self, ctx, playlist_name: str = None):
        playlist_name = await self._db.shuffle(int(ctx.message.author.id), playlist_name)
        await self._bot.whisper('**Playlist** {} **was shuffled**'.format(playlist_name))

    @staticmethod
    def _ordinal(n):
        return "%d%s" % (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])

    async def _show(self, user_id, *, playlist_name=None, start=1):
        # offset is start -1
        items, playlist_name, total = await self._db.show(user_id, start-1, 20, playlist_name)

        if not items:
            if start == 1 or total == 0:
                await self._bot.whisper('**Playlist** {} **is empty**'.format(playlist_name))
            else:
                await self._bot.whisper('**There are no songs in the playlist** {} **, starting from the** {} **song**'
                                        .format(playlist_name, self._ordinal(start)))
            return

        reply = '**{} songs (out of {}) from playlist** {}**, starting from the **{}**:**\n **>** ' \
                .format(len(items), total, playlist_name, self._ordinal(start)) + \
                '\n **>** '.join(['[{}] {}'.format(*item) for item in items])
        await self._bot.whisper(reply)
