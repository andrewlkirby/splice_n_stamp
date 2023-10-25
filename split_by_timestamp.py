from pydantic import BaseModel
from pydub import AudioSegment
from typing import List, Any, Tuple
from pathlib import Path

from loguru import logger
from time import perf_counter

import asyncio
from functools import partial
from asyncio.events import AbstractEventLoop
from concurrent.futures import ProcessPoolExecutor

class Timestamps(BaseModel):
    start: float
    stop: float

class Split(BaseModel):
    timestamps: List[Timestamps] = None
    input_path: Path = None
    output_paths_and_parts: List[Tuple[Path, Any]] = None

    @staticmethod
    def export_audio(output_filename: Path, audio_segment: Any) -> Path:
        # if Path(output_filename).suffix == 'wav':
        audio_segment.export(output_filename, format='wav')
        return output_filename

    def by_timestamp(input_path: Path, timestamps: List[Timestamps]) -> 'Split':
        part_number = 1
        filename = Path(input_path).stem
        audio_segment = AudioSegment.from_wav(input_path)
        output_paths_and_parts = []
        for ts in timestamps:
            audio_part = audio_segment[ts['start'] * 1000 : ts['stop'] * 1000]
            if ts['stop'] - ts['start'] < 1000:
                audio_part = AudioSegment.silent(duration=1000) + audio_part
            output_path = filename + '_split_pt' + str(part_number) + '.wav'
            output_paths_and_parts.append((output_path, audio_part))
            part_number += 1
        data = {
                'timestamps': timestamps,
                'input_path': input_path,
                'output_paths_and_parts': output_paths_and_parts
                }
        return Split(**data)

async def build_split(input_path: Path, 
                      timestamps: List[Timestamps],
                      max_workers = 8) -> Split:
    """
    Prepares partitions of audio clips by timestamp and outputs to current directory.
    For each partition, adds a _pt1, _pt2, etc to each output filename.
    """
    t1_start = perf_counter()
    logger.info(f"Starting splitting by timestamp!")
    data = Split.by_timestamp(input_path, timestamps)
    if data:
        parts = data.output_paths_and_parts
        with ProcessPoolExecutor(max_workers) as process_pool:
            loop: AbstractEventLoop = asyncio.get_running_loop()
            calls: List[partial[int]] = [partial(Split.export_audio, part[0], part[1]) for part in parts]
            call_coros = []
            for call in calls: call_coros.append(loop.run_in_executor(process_pool, call))
            results = await asyncio.gather(*call_coros)
            for result in results:
                logger.info(f"Export completed for file: {result}")
        t1_stop = perf_counter()
        logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
        return data
    else: pass

def run_split(input_path: Path, 
              timestamps: List[Timestamps]):
    return asyncio.run(build_split(input_path, timestamps))

if __name__ == '__main__':
    run_split()