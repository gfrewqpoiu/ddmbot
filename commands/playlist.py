import discord.ext.commands as dec
import discord.ext.commands.view as decw

import database.playlist
from discord import Member
from typing import Optional


class Playlist(dec.Cog):
    """Playlist manipulation, switching, listing playlists and their content"""
    def __init__(self, bot):
        self._bot = bot
        self._db = database.playlist.PlaylistInterface(bot.loop, bot.config['ddmbot'])

    _help_messages = {
        'group': 'Playlist manipulation, switching, listing playlists and their content\n\n'
        'All the subcommands expect \'active\', \'create\', \'delete\' and \'select\' manipulates with your active '
        'playlist. You can query your active playlist with \'playlist active\' and change it with \'playlist select\' '
        'commands.\nWith  all the other subcommands, you can optionally specify the playlist name *before* the '
        'subcommand, e.g. \'playlist [playlist_name] peek\' will list the content of a playlist with a given name. '
        'You *cannot* use this feature if your playlist name collides with a subcommand name.',

        'active': 'Displays the name of your active playlist\n\n',

        'append': 'Inserts the specified songs into your playlist\n\n'
        'Youtube, Soundcloud and Bandcamp services are supported (incl. playlists). You can specify multiple URLs or '
        'song IDs in the command arguments. Songs are inserted *at the end* of your playlist.',

        'create': 'Creates new playlist with a given name\n\n'
        'The default behaviour for a new playlist is to repeat the songs in a loop.\nPlaylist will be automatically '
        'set as your active playlist. If you want to avoid that, pass \'False\' as a second parameter.',

        'clear': 'Clears your playlist\n\n'
        'All songs from the playlist will be removed. This cannot be undone.',

        'delete': 'Removes the specified playlist\n\n'
        'Playlist is removed along with all the songs in it. This cannot be undone.',

        'list': 'Lists the available playlists\n\n'
        'List of your playlist is be returned along with the number of songs and their repeat setting.',

        'peek': 'Lists the songs in your playlist\n\n'
        'List of songs from your playlist is returned.\n\nDue to message length restrictions, up to 20 songs are '
        'returned for a single request. By default, songs from the beginning of your playlist are listed. You can list '
        'the rest of your playlist by specifying the offset with an optional argument (e.g. command with offset 17 '
        'will list songs at positions 17 to 36).',

        'pop': 'Removes specified number of songs from the head of your playlist\n\n'
        'If number is not specified, a single song is removed. Songs are removed *from the beginning* of the playlist.',

        'popid': 'Deletes the specified song from your playlist\n\n'
        'Song ID can be located in the square brackets just before the title. It is included in the status message and '
        'all the listings.',

        'prepend': 'Inserts the specified songs into your playlist\n\n'
        'Youtube, Soundcloud and Bandcamp services are supported (incl. playlists). You can specify multiple URLs or '
        'song IDs in the command arguments. Songs are inserted *at the beginning* of your playlist.',

        'repeat': 'Set repeat behaviour for your playlist\n\n'
        'You can switch between removing and repeating songs from your playlist after playing. The current setting '
        'can be queried with \'playlist list\' command, every playlist can be configured separately.\nWhen turned on, '
        'songs are simply reinserted at the end of the playlist after being played.',

        'select': 'Changes your active playlist\n\n'
        'Playlist specified will be set as your active playlist. Active playlist is the one used when playing songs '
        'from the DJ queue. Active playlist is also the one modified by other \'playlist\' commands by default',

        'shuffle': 'Shuffles songs in your playlist\n\n'
        'Randomly re-orders songs in the playlist.'
    }

    @dec.group(invoke_without_command=True, aliases=['p'], help=_help_messages['group'])
    async def playlist(self, ctx, subcommand: str, *arguments: str):
        # TODO: CHECK FOR THE PLAYLIST NAME
        if not arguments:
            raise dec.UserInputError('Command *playlist* has no subcommand named {}. Please use `{}help playlist` to '
                                     'list all the available subcommands.'.format(subcommand, ctx.prefix))
        # will try to treat subcommand as a playlist name from now on
        playlist_name = subcommand
        if not await self._db.exists(ctx.author.id, playlist_name):
            raise dec.UserInputError('Command *playlist* has no subcommand named {0}, nor is it a name of your '
                                     'playlist. Please use `{1}help playlist` to list all the available subcommands '
                                     'or `{1}playlist list` to list all your playlists.'
                                     .format(playlist_name, ctx.prefix))

        # now we try to execute a subcommand depending on the input
        #   arguments[0] == subcommand name without postfix
        #   playlist_name + arguments[1:] == subcommand arguments

        # two-step approach -- tackling aliases
        subcommand = ctx.command.get_command(arguments[0])
        if subcommand is None:
            raise dec.UserInputError('Command *playlist* has no subcommand named {}. Please use `{}help playlist` to '
                                     'list all the available subcommands.'.format(arguments[0], ctx.prefix))
        subcommand = subcommand.name
        # now try to call explicit version
        subcommand = ctx.command.get_command('{}_explicit'.format(subcommand))
        if subcommand is None:
            raise dec.UserInputError('The subcommand {} does not support optional playlist specification. Please use '
                                     '`{}help playlist` to list all the available subcommands and their arguments.'
                                     .format(arguments[0], self._bot.config['ddmbot']['delimiter']))

        # replace the string view with the swapped one and invoke the correct subcommand
        swapped_command = '{}{} {} {}'.format(ctx.prefix, ctx.invoked_with, subcommand, playlist_name)
        if arguments[1:]:
            swapped_command += ' ' + ' '.join('"{}"'.format(arg) for arg in arguments[1:])
        ctx.view = decw.StringView(swapped_command)
        ctx.view.index = len(ctx.prefix) + len(ctx.invoked_with) + len(str(subcommand)) + 1
        ctx.view.previous = ctx.view.index
        ctx.invoked_with = arguments[0]

        # now invoke
        return await subcommand.invoke(ctx)

    @playlist.command(ignore_extra=False, aliases=['a'], help=_help_messages['active'])
    async def active(self, ctx):
        name = await self._db.get_active(ctx.author.id)
        await ctx.author.send('**Active playlist:** {}'.format(name))

    @playlist.command(ignore_extra=False, aliases=['as'], help=_help_messages['append'])
    async def append(self, ctx, *uris: str):
        return await self._insert(ctx.author, uris)

    @playlist.command(ignore_extra=False, hidden=True)
    async def append_explicit(self, ctx, playlist_name: str, *uris: str):
        return await self._insert(ctx.author, uris, playlist_name=playlist_name)

    @playlist.command(ignore_extra=False, help=_help_messages['create'])
    async def create(self, ctx, playlist_name: str, set_active: bool = True):
        await self._db.create(ctx.author.id, playlist_name)
        reply = '**New playlist with the name** {} **was created**'.format(playlist_name)
        if set_active:
            await self._db.set_active(int(ctx.message.author.id), playlist_name)
            reply += '\nYour active playlist was switched to the newly created one.'
        await ctx.author.send(reply)

    @playlist.command(ignore_extra=False, help=_help_messages['clear'])
    async def clear(self, ctx):
        return await self._clear(ctx.author)

    @playlist.command(ignore_extra=False, hidden=True)
    async def clear_explicit(self, ctx, playlist_name: str):
        return await self._clear(ctx.author, playlist_name)

    async def _clear(self, author: Member, playlist_name: Optional[str] = None) -> None:
        playlist_name = await self._db.clear(author.id, playlist_name)
        await author.send('**Playlist** {} **was cleared**'.format(playlist_name))

    @playlist.command(ignore_extra=False, help=_help_messages['delete'])
    async def delete(self, ctx, playlist_name: str):
        await self._db.delete(ctx.author.id, playlist_name)
        await ctx.author.send('**Playlist** {} **was removed**'.format(playlist_name))

    @playlist.command(ignore_extra=False, aliases=['l'], help=_help_messages['list'])
    async def list(self, ctx):
        items = await self._db.list(ctx.author.id)
        if not items:
            return await ctx.author.send('**You don\'t have any playlists**')

        reply = '**You currently have {} playlist(s):**\n **>** '.format(len(items)) + \
                '\n **>** '.join(['{} ({} song(s), songs are {})'
                                 .format(item['name'], item['song_count'],
                                         'repeated' if item['repeat'] else 'removed after playing') for item in items])
        await ctx.author.send(reply)

    @playlist.command(ignore_extra=False, aliases=['p'], help=_help_messages['peek'])
    async def peek(self, ctx, start: int = 1) -> None:
        return await self._peek(ctx.author, start=start)

    @playlist.command(ignore_extra=False, hidden=True)
    async def peek_explicit(self, ctx, playlist_name: str, start: int = 1) -> None:
        return await self._peek(ctx.author, start=start, playlist_name=playlist_name)

    async def _peek(self, author: Member, *, start: int = 1, playlist_name: str = None) -> None:
        if start <= 0:
            raise dec.UserInputError('Start must be a positive number')
        # offset is start -1
        items, playlist_name, total = await self._db.show(author.id, start - 1, 20, playlist_name)

        if not items:
            if start == 1 or total == 0:
                await author.send('**Playlist** {} **is empty**'.format(playlist_name))
            else:
                await author.send(
                    '**There are no songs in the playlist** {} **, starting from the** {} **song**'
                    .format(playlist_name, self._ordinal(start)))
            return

        reply = '**{} song(s) (out of {}) from playlist** {}**, starting from the **{}**:**\n **>** ' \
                .format(len(items), total, playlist_name, self._ordinal(start)) + \
                '\n **>** '.join(['[{}] {}'.format(*item) for item in items])
        await author.send(reply)

    @playlist.command(ignore_extra=False, help=_help_messages['pop'])
    async def pop(self, ctx, count: int = 1):
        return await self._pop(ctx.author, count=count)

    @playlist.command(ignore_extra=False, hidden=True)
    async def pop_explicit(self, ctx, playlist_name: str, count: int=1):
        return await self._pop(ctx.author, count=count, playlist_name=playlist_name)

    async def _pop(self, author: Member, *, count=1, playlist_name=None):
        playlist_name, real_count = await self._db.pop(author.id, count, playlist_name)

        reply = '**{} song(s) removed from playlist {}**'.format(real_count, playlist_name)
        if real_count < count:
            reply += '\n{} song(s) could not be removed because the playlist is empty.'.format(count - real_count)
        await author.send(reply)

    @playlist.command(ignore_extra=False, help=_help_messages['popid'])
    async def popid(self, ctx, song_id: int):
        return await self._popid(ctx.author, song_id)

    @playlist.command(ignore_extra=False, hidden=True)
    async def popid_explicit(self, ctx, playlist_name: str, song_id: int):
        return await self._popid(ctx.author, song_id, playlist_name)

    async def _popid(self, author: Member, song_id, playlist_name=None):
        playlist_name = await self._db.pop_id(author.id, song_id, playlist_name)
        await author.send('**Song [{}] was removed from playlist {}**'.format(song_id, playlist_name))

    @playlist.command(ignore_extra=False, aliases=['ps'], help=_help_messages['prepend'])
    async def prepend(self, ctx, *uris: str):
        return await self._insert(ctx.author, uris, prepend=True)

    @playlist.command(ignore_extra=False, hidden=True)
    async def prepend_explicit(self, ctx, playlist_name: str, *uris: str):
        return await self._insert(ctx.author, uris, playlist_name=playlist_name, prepend=True)

    @playlist.command(ignore_extra=False, help=_help_messages['repeat'])
    async def repeat(self, ctx, repeat_policy: str):
        return await self._repeat(ctx.author, repeat_policy)

    @playlist.command(ignore_extra=False, hidden=True)
    async def repeat_explicit(self, ctx, playlist_name: str, repeat_policy: str):
        return await self._repeat(ctx.author, repeat_policy, playlist_name)

    async def _repeat(self, author: Member, repeat_policy, playlist_name=None):
        repeat_policy = repeat_policy.lower()
        if repeat_policy in ('on', 'true', '1', 'repeat'):
            setting = True
        elif repeat_policy in ('off', 'false', '0', 'remove'):
            setting = False
        else:
            await author.send('Valid options are:\n    \'on\', \'true\', \'1\', \'repeat\'\nor\n    \'off\', '
                              '\'false\', \'0\', \'remove\'\nrespectively')
            return

        playlist_name = await self._db.repeat(author.id, setting, playlist_name)
        message = '**Songs from the playlist** {} **will be '.format(playlist_name)
        if setting:
            message += "repeated after playing**"
        else:
            message += "removed after playing**"
        await author.send(message)

    @playlist.command(ignore_extra=False, aliases=['s'], help=_help_messages['select'])
    async def select(self, ctx, playlist_name: str):
        await self._db.set_active(ctx.author.id, playlist_name)
        await ctx.author.send('**Playlist** {} **was set as active**'.format(playlist_name))

    @playlist.command(ignore_extra=False, help=_help_messages['shuffle'])
    async def shuffle(self, ctx):
        return await self._shuffle(ctx.author)

    @playlist.command(ignore_extra=False, hidden=True)
    async def shuffle_explicit(self, ctx, playlist_name: str):
        return await self._shuffle(ctx.author, playlist_name)

    async def _shuffle(self, author: Member, playlist_name=None):
        playlist_name = await self._db.shuffle(author.id, playlist_name)
        await author.send('**Playlist** {} **was shuffled**'.format(playlist_name))

    async def _insert(self, author: Member, uris, playlist_name=None, prepend=False):
        # print the disclaimer
        await author.send('Please note that inserting new songs can take a while. Be patient and wait for the '
                          'result. You can run other commands, but **avoid manipulating your playlist**.')
        # now do the operation
        playlist_name, inserted, failed, truncated, messages = await self._db.insert(author.id, playlist_name, prepend,
                                                                                     uris)

        reply = '**{} song(s) inserted to** {}\n{} insertion(s) failed'.format(inserted, playlist_name, failed)
        if messages:
            reply += '\n **>** ' + '\n **>** '.join(messages[:10])
        if len(messages) > 10:
            reply += '\n **>** ... (more messages suppressed)'
        if truncated:
            reply += '\n__**Inserting was cancelled before processing the whole input.**__'

        await author.send(reply)

    @staticmethod
    def _ordinal(n):
        return "%d%s" % (n, "tsnrhtdd"[(n // 10 % 10 != 1) * (n % 10 < 4) * n % 10::4])
