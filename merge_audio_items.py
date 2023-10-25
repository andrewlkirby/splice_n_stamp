from pydub import AudioSegment
from pydantic import BaseModel
from pathlib import Path
from typing import List, Any
from loguru import logger
import regex as re
from time import perf_counter

class MergeAudioItems(BaseModel):
    input_audio_paths: List[Path] = None
    output_audio_path: Path = None

    def merge(input_audio_paths: List[Path]) -> 'MergeAudioItems':
        """
        Merges files split by this tool together.
        Relies on filenames having _pt1.wav, _pt2.wav, etc as filename. 
        """
        t1_start = perf_counter()
        logger.info(f"Starting audio item merging!")
        audio_segments = AudioSegment.empty()
        input_audio_paths = [item for item in input_audio_paths if re.search(r'_pt\d*\.wav', item) != None]
        # for sorting strings with file names like _pt9.wav, _pt10.wav, _pt11.wav correctly:
        input_audio_paths_sorted = sorted(input_audio_paths, key=lambda x: int(x.partition('_pt')[2].partition('.wav')[0]))
        for item in input_audio_paths_sorted:
            audio_type = Path(item).suffix[1:]
            if audio_type == 'wav':
                seg = AudioSegment.from_file(item, format = audio_type)
                audio_segments += seg 
            else:
                logger.info('Unsupported format! Please use wav!')
        if audio_segments != AudioSegment.empty() or None:
            if re.search(r'_pt\d*\.', input_audio_paths[0]) != None:
                merged_filename = re.sub(r'_pt\d*\.', '_merged.', input_audio_paths[0])
                audio_segments.export(merged_filename, format = audio_type)
            else:
                # merged_filename = re.sub(r'\.wav', '_merged.wav', input_audio_paths[0])
                pass
            data = {'input_audio_paths': input_audio_paths_sorted, 'output_audio_path': merged_filename}
            t1_stop = perf_counter()
            logger.info(f"Task complete! Elapsed time: {t1_stop - t1_start}")
            return MergeAudioItems(**data)

if __name__ == '__main__':
    MergeAudioItems.merge()