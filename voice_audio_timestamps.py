import contextlib
from distutils.command.build import build
import wave
import webrtcvad
from pydantic import BaseModel
from typing import Dict, List, Any, Union
from pathlib import Path
from loguru import logger
import subprocess
from time import perf_counter
import regex as re

import asyncio
from functools import partial
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from glob import glob


# set aggressiveness; 0 = beast mode aggressive, 3 = gentle
vad = webrtcvad.Vad(3)

class WaveInfo(BaseModel):
    pcm_data: Any
    sample_rate: int

class Frame(BaseModel):
    audio: bytes
    timestamp: str
    duration: float

class VoicedFrames(BaseModel):
    frame: Any
    is_speech: bool
    time: float

class Timestamps(BaseModel):
    start: float
    stop: float

class VoiceDetect(BaseModel):
    path: Path = None
    # wave_info: WaveInfo = None
    # voiced_frames: List[VoicedFrames] = None
    timestamps: List[Timestamps] = None

    @staticmethod
    def read_wave(path: Path) -> WaveInfo:
      """
      Reads a .wav file.
      Takes the path, and returns (PCM audio data, sample rate).
      """
      path_ = Path(path)
      if path_.suffix == '.wav':
          with contextlib.closing(wave.open(path, 'rb')) as wf:
              num_channels = wf.getnchannels()
              assert num_channels == 1
              sample_width = wf.getsampwidth()
              assert sample_width == 2
              sample_rate = wf.getframerate()
              assert sample_rate in (8000, 16000, 32000, 48000)
              pcm_data = wf.readframes(wf.getnframes())
              data = {'pcm_data': pcm_data, 'sample_rate': sample_rate}
              return WaveInfo(**data)
      else:
          logger.info('Wrong filetype! Requires .wav')

    @staticmethod
    def frame_generator(audio, sample_rate, frame_duration_ms: int = 30):
        """
        Generates audio frames from PCM audio data.
        Takes the desired frame duration in milliseconds, the PCM data, and
        the sample rate.
        Yields Frames of the requested duration.
        """
        n = int(sample_rate * (frame_duration_ms / 1000.0) * 2)
        offset = 0
        timestamp = 0.0
        duration = (float(n) / sample_rate) / 2.0
        frames = []
        while offset + n < len(audio):
            data = {
                    'audio': audio[offset:offset + n],
                    'timestamp': timestamp,
                    'duration': duration
                    }
            frames.append(data)
            timestamp += duration
            offset += n
        return frames

    @staticmethod
    def get_voiced_frames(frames, sample_rate: int, frame_duration_ms: int = 30) -> List[VoicedFrames]:
        frame_duration_seconds = 0
        voiced_frames_data = []
        for frame in frames:
            is_speech = vad.is_speech(frame['audio'], sample_rate)
            frame_duration_seconds += frame_duration_ms / 1000
            data = {
                    'frame': frame,
                    'is_speech': is_speech,
                    'time': round(frame_duration_seconds, 3)
                    }
            voiced_frames_data.append(data)
        return voiced_frames_data

    @staticmethod
    def get_timestamps(voiced_frames: VoicedFrames) -> List[Timestamps]:
        voice_times = []
        timestamps = []
        for item in voiced_frames:
            if item['is_speech'] == True:
                voice_times.append(item['time'])
            else:
                if len(voice_times) > 0:
                    data = {
                            'start': voice_times[0],
                            'stop': voice_times[-1]
                            }
                    timestamps.append(data)
                    voice_times.clear()
        return timestamps

    def do_timestamps(self, path: Path) -> 'VoiceDetect':
        logger.info("Starting voice detection!")
        path_ = Path(path)
        if path_.suffix == '.wav':
            t1_start = perf_counter()
            wave_info = self.read_wave(path)
            frames = self.frame_generator(wave_info.pcm_data, wave_info.sample_rate)
            voiced_frames = self.get_voiced_frames(frames, wave_info.sample_rate)
            timestamps = self.get_timestamps(voiced_frames)
            data = {
                    'path': path,
                    # 'wave_info': wave_info.sample_rate,
                    # 'voiced_frames': voiced_frames,
                    'timestamps': timestamps
                    }
            t1_stop = perf_counter()
            logger.info(f"Done! Elapsed time: {t1_stop - t1_start}")
            return VoiceDetect(**data)
        else:
            logger.info(f"{path_.as_posix()} is not a supported filetype!")


# NOTE: Do we want these outputs to go to JSON files? Probably!
async def build_voice_detection(wav_directory, max_workers=8) -> VoiceDetect:
    """
    Puts voice detection into process pool and adds asynchronous return of results (timestamps).
    max_workers is the number of cores you want to have going on this task. Default is 8.
    """
    # from Chapter 6 in Python Concurrency book
    t1_start = perf_counter()
    with ProcessPoolExecutor(max_workers) as process_pool:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        wav_files = glob(wav_directory)
        calls: List[partial[int]] = [partial(VoiceDetect().do_timestamps, wav) for wav in wav_files]
        call_coros = []
        for call in calls: call_coros.append(loop.run_in_executor(process_pool, call))
        results = await asyncio.gather(*call_coros)
        for result in results:
            logger.info(f"Timestamps completed for file: {result.path}")
    t1_stop = perf_counter()
    logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
    return results

def run_voice_detection(wav_directory = None):
    return asyncio.run(build_voice_detection(wav_directory))

if __name__ == '__main__':
    run_voice_detection()