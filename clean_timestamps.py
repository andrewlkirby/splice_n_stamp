from pydantic import BaseModel
from pathlib import Path
from typing import List
from loguru import logger
from time import perf_counter

# import these, maybe
class Timestamps(BaseModel):
    start: float
    stop: float

class VoiceDetect(BaseModel):
    path: Path
    timestamps: List[Timestamps]

class CleanTimestamps(BaseModel):
    path: Path = None
    raw_timestamps: List[Timestamps] = None
    clean_timestamps: List[Timestamps] = None

    def clean(raw_timestamps: VoiceDetect, timestamp_merge_window: int = 1) -> 'CleanTimestamps':
        """
        Merges timestamps that are within a time window together, to consolidate values.
        Default time window = 1 second 
        """
        t1_start = perf_counter()
        logger.info(f"Starting timestamp cleansing!")
        clean_timestamps = []
        ts_items = [item.dict() for item in raw_timestamps.timestamps]
        for item in ts_items:
            # padding: -- consider making this an arg
            # item['start'] - 1 # don't let this be a negative value
            # item['stop'] + 1
            if len(clean_timestamps) == 0:
                clean_timestamps.append(item)
            else:
                if item['start'] - clean_timestamps[-1]['stop'] < timestamp_merge_window: 
                    # default is 1 second to merge for timestamp_merge_window
                    clean_timestamps[-1]['stop'] = item['stop']
                else:
                    clean_timestamps.append(item)
        data = {'path': raw_timestamps.path, 'raw_timestamps': raw_timestamps.timestamps, 'clean_timestamps': clean_timestamps}
        t1_stop = perf_counter()
        logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
        return CleanTimestamps(**data)

if __name__ == '__main__':
    CleanTimestamps.clean()