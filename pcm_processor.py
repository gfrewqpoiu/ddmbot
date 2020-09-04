from loguru import logger
import trio
import os
import audioop
import errno
import fcntl
import trio_util
import trio_asyncio
import discord
import logging

FCNTL_F_LINUX_BASE = 1024
FCNTL_F_SETPIPE_SZ = FCNTL_F_LINUX_BASE + 7

log = logging.getLogger('ddmbot.pcm')


class PcmProcessor:
    def __init__(self, bot, next_callback):
        logger.debug("Initializing PCM Processor")
        self._bot = bot
        config = bot.config['ddmbot']

        pipe_size = int(config['pcm_pipe_size'])
        if pipe_size > 2 ** 31 or pipe_size <= 0:
            raise ValueError('Provided \'pcm_pipe_size\' is invalid')

        if not callable(next_callback):
            raise TypeError('Next callback must be a callable object')

        # despite the fact we expect voice_client to change, encoder parameters should be static
        # grabbed these from the old discord opus.py
        self._frame_len = 3840  # was encoder.frame_size
        self._frame_period = 20 / 1000.0  # was encoder.frame_length / 1000.0
        self._volume = int(config['default_volume']) / 100

        logger.debug("Creating PCM Pipe")
        self._in_pipe_fd = os.open(config['pcm_pipe'], os.O_RDONLY | os.O_NONBLOCK)
        logger.debug("Creating internal Pipe")
        self._out_pipe_fd = os.open(config['int_pipe'], os.O_WRONLY | os.O_NONBLOCK)

        try:

            fcntl.fcntl(self._in_pipe_fd, FCNTL_F_SETPIPE_SZ, pipe_size)
        except OSError as e:
            if e.errno == 1:
                raise RuntimeError('Required PCM pipe size is over the system limit, see \'pcm_pipe_size\' in the '
                                   'configuration file') from e
            raise e

        self._next = next_callback
        self._end = trio.Event()
        self._is_running = trio_util.AsyncBool()

    @property
    def volume(self):
        return self._volume

    @volume.setter
    def volume(self, value):
        self._volume = min(max(value, 0.0), 2.0)

    async def stop(self) -> None:
        """Stops the PCM Processor."""
        logger.debug("Stopping PCM Processor.")
        if not self._end.is_set():
            self._end.set()
        if self._is_running.value:
            await self._is_running.wait_value(False)
        self.flush()
        os.close(self._in_pipe_fd)
        os.close(self._out_pipe_fd)

    def flush(self) -> None:
        """Clears out the pipes from all audio data."""
        logger.debug("Flushing PCM Pipe from all data.")
        try:
            os.read(self._in_pipe_fd, 1048576)
        except OSError as e:
            if e.errno != errno.EAGAIN:
                raise

    def _run(self, buffering_cycles, zero_data, next_called, cycles_in_second):
        # set initial value for data length
        data_len = 0

        # if it's not a buffering cycle read more data
        if buffering_cycles:
            logger.trace("Buffering")
            buffering_cycles -= 1
            data = zero_data
        else:
            try:
                logger.trace("Reading one frame of data.")
                data = os.read(self._in_pipe_fd, self._frame_len)
                data_len = len(data)
                if data_len:
                    next_called = False

                if data_len != self._frame_len:
                    if data_len == 0:
                        # if we read nothing, that means the input to the pipe is not connected anymore
                        if not next_called:
                            logger.trace("Got no data. Going to next song.")
                            next_called = True
                            self._next()
                        data = zero_data
                    else:
                        # if we read something, we are likely at the end of the input, pad with zeroes and log
                        # TODO: is there a way to distinguish buffering issues and end of the input issues?
                        logger.debug('PcmProcessor: Data was padded with zeroes. End of Song?')
                        data.ljust(self._frame_len, b'\0')

            except OSError as e:
                if e.errno == errno.EAGAIN:
                    logger.debug("Got EAGAIN Error while reading data.")
                    data = zero_data
                    log.warning('PcmProcessor: Buffer not ready, waiting one second')
                    buffering_cycles = cycles_in_second
                else:
                    raise

        # now we try to pass data to the output, if connected, we also send the silence (zero_data)
        # This section is responsible for the direct stream.
        if self._bot.stream.is_connected():
            try:
                logger.trace("Sending data to direct stream pipe.")
                os.write(self._out_pipe_fd, data)
                # data sent successfully, clear the congestion flag
                output_congestion = False
            except OSError as e:
                if e.errno == errno.EAGAIN:
                    # prevent spamming the log with megabytes of text
                    if not output_congestion:
                        log.error('PcmProcessor: Output pipe for direct stream not ready, dropping frame(s)')
                        output_congestion = True
                else:
                    raise
        else:
            # if we are not connected, there is no output congestion and the underlying buffer will be cleared
            output_congestion = False

        # and last but not least, discord output, this time, we can (should) omit partial frames or zero data
        voice_client = self._bot.voice
        if voice_client.is_connected() and data_len == self._frame_len:
            if not discord.opus.is_loaded():
                raise EnvironmentError("Discord OPUS Library wasn't loaded.")
            # adjust the volume
            data = audioop.mul(data, 2, self._volume)
            # call the callback
            logger.trace("Sending full frame of audio data to Discord.")
            voice_client.send_audio_packet(data)

    @logger.catch(reraise=True)
    async def run(self) -> None:
        """Actually plays the music. (TRIO)"""
        try:
            self._is_running.value = True
            logger.debug("PCM Processor is running.")
            next_called = True  # variable to prevent constant calling of self._next()
            output_congestion = False  # to control log spam
            buffering_cycles = 0
            cycles_in_second = 1 // self._frame_period
            zero_data = b'\0' * self._frame_len  # 3840 zero bytes

            async for _ in trio_util.periodic(self._frame_period):
                if self._end.is_set():
                    break
                await trio.to_thread.run_sync(self._run, buffering_cycles, zero_data, next_called, cycles_in_second)
        finally:
            self._is_running.value = False
            await self.stop()


processor: PcmProcessor


async def run(bot, callback):
    global processor
    processor = PcmProcessor(bot, callback)
    async with trio.open_nursery() as nursery:
        nursery.start_soon(processor.run)


async def stop():
    if processor is not None:
        await processor.stop()
