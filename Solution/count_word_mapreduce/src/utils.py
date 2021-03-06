import re
import os
from pathlib import Path
from collections import Counter, defaultdict

from argparse import ArgumentParser

INPUT_DIR = os.path.join(os.getcwd(), "data/inputs")
OUTPUT_DIR = os.path.join(os.getcwd(), "data/outputs")
INTERMEDIATE_DIR = os.path.join(os.getcwd(), "data/intermediate")


def driver_arg_parse(parser=None):
    if parser is None:
        parser = ArgumentParser()
    parser.add_argument("-n", type=int, default=5,
                        help="Number of Map operations")
    parser.add_argument("-m", type=int, default=5,
                        help="Number of Reduce operations")
    return parser


def collect_map_tasks():
    return os.listdir(INPUT_DIR)


def collect_reduce_tasks():
    return os.listdir(INTERMEDIATE_DIR)


def read_file_linebyline(f):
    return Path(f).read_text().splitlines()


def do_map(input_file, M, map_id, input_dir=INPUT_DIR,
           output_dir=INTERMEDIATE_DIR):
    if map_id != -1:
        print(f"Working on {input_file} with map id {map_id} and {M} reduceable buckets")
        buckets = defaultdict(list)
        text = Path(input_dir, input_file).read_text()
        all_words = list(re.findall("(?:\w|['-]\w)+", text))
        for word in all_words:
            buckets[ord(word[0]) % M].append(word)
        for key, value in buckets.items():
            text = "\n".join(value)
            path = Path(output_dir, f"mr-{map_id}-{key}")
            with path.open("a") as f:
                f.write(text)
                f.write("\n")
        print(f"Map Task completed for {input_file}")
    return


def do_reduce(reduce_id, N, input_dir=INTERMEDIATE_DIR,
              output_dir=OUTPUT_DIR):
    if reduce_id != -1:
        print(f"Working on reducing {N} mapped tasks for reduce id {reduce_id}")
        files_to_reduce = []
        for n in range(N):
            files_to_reduce.append(os.path.join(input_dir, f"mr-{n}-{reduce_id}"))
        # all_words = []
        # start = time.time()
        # with Pool(5) as p:
        #     all_words = list(chain(*p.map(read_file_linebyline, files_to_reduce)))
        all_words = []
        for f in files_to_reduce:
            all_words += Path(f).read_text().splitlines()
        word_counts = Counter(all_words)
        output_file = os.path.join(output_dir, f"out-{reduce_id}")
        with open(output_file, 'w+') as f:
            for word, count in word_counts.items():
                f.write(f"{word} {count}\n")
        print(f"Reduce task completed for reduce id {reduce_id}")
    return