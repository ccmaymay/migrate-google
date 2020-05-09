#!/usr/bin/env python3

import json
from humanfriendly import format_size
from collections import Counter

from migrate_google import configure_logging, LOGGER

MISSING_ENTRY_NAME = '???'


def add_hierarchical_info(root_id, file_metadata_map):
    top_down_stack = [root_id]
    bottom_up_stack = []
    while top_down_stack:
        file_id = top_down_stack.pop()
        file_metadata = file_metadata_map[file_id]

        if 'name' not in file_metadata:
            file_metadata['name'] = MISSING_ENTRY_NAME

        if not file_metadata.get('parents'):
            file_metadata['parents'] = []

        if file_metadata.get('size'):
            file_metadata['size'] = int(file_metadata['size'])
        else:
            file_metadata['size'] = 0

        path = file_metadata['name']
        parents = file_metadata['parents']
        if parents:
            path = '{}/{}'.format(file_metadata_map[parents[0]]['path'], path)
        file_metadata['path'] = path

        bottom_up_stack.append(file_id)

        for child_id in file_metadata['children']:
            top_down_stack.append(child_id)

    while bottom_up_stack:
        file_metadata = file_metadata_map[bottom_up_stack.pop()]
        for child_id in file_metadata['children']:
            file_metadata['size'] += file_metadata_map[child_id]['size']


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Load and summarize metadata from drive files')
    parser.add_argument('input_path', help='Path to input jsonl metadata file')
    parser.add_argument('--num-top-files', type=int, default=100,
                        help='Number of top (largest) files to show')
    args = parser.parse_args()

    configure_logging()

    LOGGER.info('Loading data from {}'.format(args.input_path))
    file_metadata_map = dict()
    with open(args.input_path) as f:
        for line in f:
            file_metadata = json.loads(line)
            file_id = file_metadata['id']

            if file_metadata['error']:
                LOGGER.warning('Error downloading metadata for {}'.format(
                    file_metadata['name']))

            if file_id not in file_metadata_map:
                file_metadata_map[file_id] = dict(children=[])
            file_metadata_map[file_id].update(file_metadata)

            if file_metadata.get('parents'):
                for p in file_metadata['parents']:
                    if p not in file_metadata_map:
                        file_metadata_map[p] = dict(children=[])
                    file_metadata_map[p]['children'].append(file_id)

    LOGGER.info('Computing hierarchical info for each root dir')
    for (file_id, file_metadata) in file_metadata_map.items():
        if not file_metadata.get('parents'):
            add_hierarchical_info(file_id, file_metadata_map)

    LOGGER.info('Checking for multiple parents')
    for (file_id, file_metadata) in file_metadata_map.items():
        parents = file_metadata['parents']
        if len(parents) > 1:
            LOGGER.warning('{} parents: {}'.format(
                len(parents), file_metadata_map[file_id]['path']))

    LOGGER.info('Checking for duplicate directory entries')
    for (file_id, file_metadata) in file_metadata_map.items():
        child_counts = Counter(
            file_metadata_map[child_id]['name']
            for child_id
            in file_metadata['children'])
        for (name, count) in child_counts.most_common():
            if count == 1:
                break
            child_ids = [
                file_metadata_map[child_id]['id']
                for child_id
                in file_metadata['children']
                if file_metadata_map[child_id]['name'] == name]
            LOGGER.warning('Duplicate entry: {}'.format(
                file_metadata_map[child_ids[0]]['path']))

    LOGGER.info('Listing {} largest files by size'.format(args.num_top_files))
    file_ids_by_size = sorted(
        file_metadata_map,
        key=lambda file_id: file_metadata_map[file_id]['size'], reverse=True)
    for file_id in file_ids_by_size[:args.num_top_files]:
        file_metadata = file_metadata_map[file_id]
        LOGGER.info('{:<8} {}'.format(format_size(file_metadata['size']), file_metadata['path']))


if __name__ == '__main__':
    main()
