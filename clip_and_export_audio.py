from pydub import AudioSegment
from pathlib import Path
from loguru import logger
from pydantic import BaseModel
from typing import List, Any, Tuple
from time import perf_counter

import asyncio
from functools import partial
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor
from glob import glob

class Clips(BaseModel):
    input_path: Path = None
    output_paths_and_parts: List[Tuple[Path, Any]] = None

    @staticmethod
    def export_audio(output_filename: Path, audio_segment: Any) -> Path:
        if Path(output_filename).suffix == '.mp3':
            audio_segment.export(output_filename, format='mp3')
        return output_filename

    def get_audio_partitions(input_path: Path, cutoff_threshold: int = 3600, chunk_duration: int = 1800) -> 'Clips':
        filename = Path(input_path).stem
        if Path(input_path).suffix == '.mp3':
            audio_segment = AudioSegment.from_mp3(input_path)
        # if Path(path).suffix == '.wav':
        #     audio_segment = AudioSegment.from_wav(path)
        if audio_segment.duration_seconds > cutoff_threshold: # cutoff default 3600
            num_chunks = audio_segment.duration_seconds / chunk_duration # chunk default 1800
            chunk_time_count = 0
            part_number = 1
            output_paths_and_parts = []
            total_time_ms = audio_segment.duration_seconds * 1000 # convert to milliseconds
            chunk_time_increment = chunk_duration * 1000
            for _ in range(int(num_chunks + 1)):
                if chunk_time_count < total_time_ms:
                    audio_part = audio_segment[chunk_time_count:chunk_time_count + chunk_time_increment]
                else:
                    audio_part = audio_segment[chunk_time_count - chunk_time_increment:total_time_ms]
                    print(chunk_time_count - chunk_time_increment)
                    print(total_time_ms)
                output_path = filename + '_pt' + str(part_number) + '.mp3'
                # part.export(output_path, format='mp3')
                output_paths_and_parts.append((output_path, audio_part))
                chunk_time_count += chunk_time_increment
                part_number += 1
            data = {
                    'input_path': input_path,
                    'output_paths_and_parts': output_paths_and_parts
                    }
            return Clips(**data)
        else: 
            logger.info('Doing nothing to this file; it is smaller than the cutoff threshold')
            data = {
                    'input_path': input_path,
                    'output_paths_and_parts': (input_path, None)
                    }
            return Clips(**data)
            

async def build_clips(input_path: Path, 
                      cutoff_threshold: int = 3600, 
                      chunk_duration: int = 1800, 
                      max_workers: int = 8) -> Clips:
    """
    Prepares partitions of audio clips and exports to current directory.
    For each partition, adds a _pt1, _pt2, etc to each output filename.
    """
    t1_start = perf_counter()
    logger.info(f"Starting clip & export!")
    parts = Clips.get_audio_partitions(input_path, cutoff_threshold, chunk_duration)
    if parts:
        parts = parts.output_paths_and_parts
        with ProcessPoolExecutor(max_workers) as process_pool:
            loop: AbstractEventLoop = asyncio.get_running_loop()
            calls: List[partial[int]] = [partial(Clips.export_audio, part[0], part[1]) for part in parts]
            call_coros = []
            for call in calls: call_coros.append(loop.run_in_executor(process_pool, call))
            results = await asyncio.gather(*call_coros)
            for result in results:
                logger.info(f"Export completed for file: {result}")
        t1_stop = perf_counter()
        logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
        return parts
    else: pass

def run_clips(input_path: Path = None, 
              cutoff_threshold: int = 3600, 
              chunk_duration: int = 1800):
    return asyncio.run(build_clips(input_path, cutoff_threshold, chunk_duration))

if __name__ == '__main__':
    run_clips()
