if __name__ == "__main__":
    # NOTE: in VSCode, need to go to Preferences -> Settings -> Python -> Terminal: Execute In File Dir
    # Create output directory
    import os
    from pathlib import Path
    import json
    from loguru import logger
    import shutil
    from glob import glob
    from time import perf_counter

    from convert_audio import run_convert
    from voice_audio_timestamps import run_voice_detection
    from clean_timestamps import CleanTimestamps
    from split_by_timestamp import run_split
    from merge_audio_items import MergeAudioItems

    t1_start = perf_counter()
    output_directory = r'output'
    if not os.path.isdir(output_directory):
        os.makedirs(output_directory)
    os.chdir(output_directory)

    # Convert audio to wav for voice detection
    # audio_items = run_convert(r'/Users/andrewkirby/Documents/summa_linguae/WER_test/Test files_Hindi/*.mp3', 'wav')

    # Detect voice and output timestamps from wav file
    wav_paths = r'/Users/andrewkirby/Documents/summa_linguae/WER_test/Test files_Eng/*_1channel.wav'
    ts_items = run_voice_detection(wav_paths)

    # clean up timestamps
    ts_items_clean = [CleanTimestamps.clean(ts_item) for ts_item in ts_items]

    # Split audio by timestamp; export timestamps to JSON
    for items in ts_items_clean:
        item_output_dirname = (Path(items.path)).stem
        if not os.path.isdir(item_output_dirname):
            os.makedirs(item_output_dirname)
        
        out_path = item_output_dirname + r'.jsonl'
        with open(out_path, 'w') as fout:
            fname_counter = 1
            for ts in items.clean_timestamps:
                ts_dict = ts.dict()
                ts_dict['filename'] = item_output_dirname + '_split_pt' + str(fname_counter) + '.wav'
                # NOTE: put vtt stuff here:
                ts_dict['vtt'] = 'Voice text from Assembly'
                fout.write(json.dumps(ts_dict) + '\n')
                fname_counter += 1
        fname_counter = 0
        logger.info(f"JSON exported: {out_path}")
        ts_d = [item.dict() for item in items.clean_timestamps]
        split_items = run_split(items.path, ts_d)
        for f in glob(str(item_output_dirname) + '*'):
            shutil.move(f, item_output_dirname) 
    
    # Set terminal directory back to above output:
    path_parent = os.path.dirname(os.getcwd())
    os.chdir(path_parent)

    # Merge split items back together
    # NOTE: can add optimization here
    # directory_items = glob(r'output/*')
    # for item in directory_items:
    #     files = glob(item + r'/*')
    #     MergeAudioItems.merge(files)

    # t1_stop = perf_counter()
    # logger.info(f"Done! Elapsed time: {t1_stop - t1_start}")


    # Cut large audio files into pieces
    # from clip_and_export_audio import run_clips
    # from glob import glob

    # directory = r'/Users/andrewkirby/Documents/summa_linguae/split_for_subham/*.mp3'
    # for path in glob(directory):
    #     run_clips(path)

    # send timestamps to sample json
    # import json
    # with open(r'sample.jsonl', 'w') as f:
    #     f.write('[\n')
    #     for item in ts_items[0].timestamps:
    #         f.write(json.dumps(item.dict())+',\n')
    #     f.write(']')