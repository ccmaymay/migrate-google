#!/usr/bin/env python3

import os
import json
import time
from humanfriendly import format_size
from collections import defaultdict

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, configure_logging

MISSING_ENTRY_NAME = '[???????]'


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
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('email', help='Email address to use when filtering')
    parser.add_argument('input_path', help='Path to input jsonl metadata file')
    parser.add_argument('--num-top-files', type=int, default=100,
                        help='Number of top (largest) files to show')
    parser.add_argument('--sleep', type=float,
                        help='Amount of time to sleep between modifying files')
    parser.add_argument('--delete-duplicates', action='store_true',
                        help='Delete all but one copy of each file this email address owns')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('summarize-drive-metadata-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()

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

    LOGGER.info('Checking for non-folders with no parents')
    for (file_id, file_metadata) in file_metadata_map.items():
        if not file_metadata['parents'] and file_metadata.get('mimeType') != 'application/vnd.google-apps.folder':
            LOGGER.warning('No parents: {}'.format(
                file_metadata_map[file_id]['path']))

    LOGGER.info('Checking for multiple parents')
    for (file_id, file_metadata) in file_metadata_map.items():
        parents = file_metadata['parents']
        if len(parents) > 1:
            LOGGER.warning('{} parents: {}'.format(
                len(parents), file_metadata_map[file_id]['path']))

    LOGGER.info('Checking for duplicate entries')
    file_counts = defaultdict(set)
    for (file_id, m) in file_metadata_map.items():
        file_counts[(
            m['name'],
            m['path'],
            tuple(sorted(m['parents'])),
            m['size'],
            tuple(sorted(
                (
                    p['type'],
                    p.get('role'),
                    p.get('emailAddress'),
                ) for p in m.get('permissions', [])
            )),
        )].add(file_id)
    for (t, file_ids) in file_counts.items():
        if len(file_ids) > 1 and t[0] != MISSING_ENTRY_NAME and ('user', 'owner', args.email) in t[-1]:
            LOGGER.warning('{} copies of entry: {}'.format(
                len(file_ids), t[1]))
            if args.delete_duplicates:
                try:
                    retrieved_file_ids = [
                        files.get(fileId=file_id).execute()['id'] for file_id in file_ids]
                    if sorted(retrieved_file_ids) == sorted(file_ids):
                        for file_id in retrieved_file_ids[1:]:
                            LOGGER.warning('Deleting {}'.format(file_id))
                            files.delete(fileId=file_id)
                except HttpError as ex:
                    LOGGER.warning('Caught exception: {}'.format(ex))

                time.sleep(args.sleep)

    LOGGER.info('Listing {} largest files by size'.format(args.num_top_files))
    file_ids_by_size = sorted(
        file_metadata_map,
        key=lambda file_id: file_metadata_map[file_id]['size'], reverse=True)
    for file_id in file_ids_by_size[:args.num_top_files]:
        file_metadata = file_metadata_map[file_id]
        LOGGER.info('{:<8} {}'.format(format_size(file_metadata['size']), file_metadata['path']))


if __name__ == '__main__':
    main()
