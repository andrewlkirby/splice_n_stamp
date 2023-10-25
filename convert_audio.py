from pydantic import BaseModel
from pathlib import Path
import regex as re
import subprocess
from typing import Union, Any, List
from time import perf_counter
from loguru import logger

import asyncio
from functools import partial
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from glob import glob

class AudioConversion(BaseModel):
    input_path: Path = None
    output_path: Path = None

    @staticmethod
    def m4a_to_wav(input_path: Union[Path, Any]) -> Union[Path, Any]:
        out_path = re.findall(r'.*\.m4a$', input_path)[0]
        output_path = re.sub(r'm4a$', 'wav', out_path)
        # bash: ffmpeg -i input.mp4 output.wav
        subprocess.call(['ffmpeg', '-i', input_path, out_path])
        return output_path

    @staticmethod
    def mp3_to_wav(input_path: Union[Path, Any]) -> Union[Path, Any]:
        output_path = Path(input_path).stem + r'.wav'
        # bash: ffmpeg -ss 00:00:00 -i input_path  out_path
        subprocess.call(['ffmpeg', '-ss', '00:00:00', '-i', input_path, '-ac', '1', '-ar', '16000', output_path])
        return output_path

    @staticmethod
    def wav_to_mp3(input_path: Union[Path, Any]) -> Union[Path, Any]:
        # bash: ffmpeg -i input.wav -vn -ar 44100 -ac 2 -b:a 192k output.mp3
        output_path = Path(input_path).stem + r'.mp3'
        subprocess.call(['ffmpeg', '-i', input_path, '-vn', '-ar', '48000', '-ac', '2', '-b:a', '192k', output_path])
        return output_path

    def convert(self, path: Union[Path, Any], input_type: str) -> 'AudioConversion':
        """
        Converts audio with FFMPEG; converted audio lives in 
        current directory. Args: path to be converted, input file type.
        Input type can be: mp3 wav m4a
        """
        logger.info("Starting audio conversion!")
        t1_start = perf_counter()
        path_ = Path(path)
        data = {'input_path': path_}
        if input_type == 'm4a':
            if path_.suffix == '.m4a':
                logger.info("Converting m4a to wav!")
                data['output_path'] = self.m4a_to_wav(path)
                t1_stop = perf_counter()
        if input_type == 'mp3':
            if path_.suffix == '.mp3':
                logger.info("Converting mp3 to wav!")
                data['output_path'] = self.mp3_to_wav(path)
                t1_stop = perf_counter()     
        if input_type == 'wav':
            if path_.suffix == '.wav':
                logger.info("Converting wav to mp3!")
                data['output_path'] = self.wav_to_mp3(path)
                t1_stop = perf_counter()
        return AudioConversion(**data)

async def build_convert(audio_file_directory: Union[Path, Any], input_type: str, max_workers=8) -> AudioConversion:
    t1_start = perf_counter()
    with ProcessPoolExecutor(max_workers) as process_pool:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        audio_files = glob(audio_file_directory)
        calls: List[partial[int]] = [partial(AudioConversion().convert, audio, input_type) for audio in audio_files]
        call_coros = []
        for call in calls: call_coros.append(loop.run_in_executor(process_pool, call))
        results = await asyncio.gather(*call_coros)
        for result in results:
            logger.info(f"Conversion completed. Output file: {result.output_path}")
    t1_stop = perf_counter()
    logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
    return results

def run_convert(audio_directory = None, input_type = None):
    return asyncio.run(build_convert(audio_directory, input_type))

if __name__ == '__main__':
    run_convert()