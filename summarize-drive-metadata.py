#!/usr/bin/env python3

import json
from collections import Counter

from migrate_google import configure_logging, LOGGER


def format_path(file_id, file_metadata_map):
    file_metadata = file_metadata_map[file_id]
    path = file_metadata['name']
    if file_metadata['parents']:
        return '{}/{}'.format(format_path(file_metadata['parents'][0], file_metadata_map), path)
    else:
        return path


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Load and summarize metadata from drive files')
    parser.add_argument('input_path', help='Path to input jsonl metadata file')
    args = parser.parse_args()

    configure_logging()

    LOGGER.info('Loading data from {}'.format(args.input_path))
    file_metadata_map = dict()
    with open(args.input_path) as f:
        for line in f:
            file_metadata = json.loads(line)
            if file_metadata['id'] not in file_metadata_map:
                file_metadata_map[file_metadata['id']] = dict(children=[])
            file_metadata_map[file_metadata['id']].update(file_metadata)
            for p in file_metadata.get('parents', ()):
                if p not in file_metadata_map:
                    file_metadata_map[p] = dict(children=[])
                file_metadata_map[p]['children'].append(file_metadata)

    LOGGER.info('Checking for multiple parents')
    for (file_id, file_metadata) in file_metadata_map.items():
        parents = file_metadata.get('parents', ())
        if len(parents) > 1:
            LOGGER.warning('{} parents: {}'.format(
                len(parents), format_path(file_id, file_metadata_map)))

    LOGGER.info('Checking for duplicate directory entries')
    for (file_id, file_metadata) in file_metadata_map.items():
        child_counts = Counter(c['name'] for c in file_metadata['children'])
        for (name, count) in child_counts.most_common():
            if count == 1:
                break
            child_ids = [c['id'] for c in file_metadata['children'] if c['name'] == name]
            LOGGER.warning('Duplicate entry: {}'.format(
                format_path(child_ids[0], file_metadata_map)))


if __name__ == '__main__':
    main()
