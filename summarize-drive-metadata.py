#!/usr/bin/env python3

import os
import json
import time
from collections import defaultdict

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, configure_logging, DriveFiles


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
                        help='Delete all but one copy of each file')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('summarize-drive-metadata-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()

    LOGGER.info('Loading data from {}'.format(args.input_path))
    drive_files = DriveFiles()
    with open(args.input_path) as f:
        for line in f:
            df = drive_files.add(json.loads(line))
            if df.error:
                LOGGER.warning('Error downloading metadata for {}'.format(df))

    LOGGER.info('Checking for trashed files')
    for df in drive_files.list():
        if df.trashed:
            LOGGER.warning('Trashed: {}'.format(df))

    LOGGER.info('Checking for files with no names')
    for df in drive_files.list():
        if df.metadata.get('name') is None:
            LOGGER.warning('No name: {}'.format(df))

    LOGGER.info('Checking for top-level entries beyond root')
    root_id = files.get(fileId='root').execute()['id']
    LOGGER.info('Root id: {}'.format(root_id))
    for df in drive_files.list():
        if df.id != root_id and not df.parents:
            LOGGER.warning('Top-level but not root: {}'.format(df))

    LOGGER.info('Checking for multiple parents')
    for df in drive_files.list():
        parents = df.parents
        if len(parents) > 1:
            LOGGER.warning('{} parents: {}'.format(len(parents), df))

    LOGGER.info('Checking for duplicate content')
    checksum_counts = defaultdict(list)
    for df in drive_files.list():
        checksum_counts[df.md5_checksum].append(df.path)
    for (cs, paths) in checksum_counts.items():
        if len(paths) > 1:
            LOGGER.warning('{} copies of content here and elsewhere: {}'.format(len(paths), paths[0]))

    LOGGER.info('Checking for duplicate content and metadata')
    metadata_counts = defaultdict(list)
    for df in drive_files.list():
        metadata_counts[(
            df.path,
            tuple(sorted(df.parent_ids)),
            df.size,
            df.md5_checksum,
            tuple(sorted(
                (
                    p['type'],
                    p.get('role'),
                    p.get('emailAddress'),
                ) for p in df.permissions
            )),
        )].append(df.id)
    for (md, file_ids) in metadata_counts.items():
        if len(file_ids) > 1 and ('user', 'owner', args.email) in md[-1]:
            LOGGER.warning('{} copies of path: {}'.format(len(file_ids), md[0]))
            if args.delete_duplicates:
                try:
                    retrieved_file_ids = [
                        files.get(fileId=file_id).execute()['id'] for file_id in file_ids]
                    if sorted(retrieved_file_ids) == sorted(file_ids):
                        for file_id in retrieved_file_ids[1:]:
                            LOGGER.warning('Deleting {}'.format(file_id))
                            files.delete(fileId=file_id).execute()
                except HttpError as ex:
                    LOGGER.warning('Caught exception: {}'.format(ex))

                time.sleep(args.sleep)

    LOGGER.info('Listing {} largest files by size'.format(args.num_top_files))
    files_by_size = sorted(drive_files.list(), key=lambda df: df.size, reverse=True)
    for df in files_by_size[:args.num_top_files]:
        LOGGER.info('{:<8} {}'.format(df.human_friendly_size, df))


if __name__ == '__main__':
    main()
