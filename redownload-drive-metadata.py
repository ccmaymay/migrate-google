#!/usr/bin/env python3

import os
import json
import time

from googleapiclient.discovery import build

from migrate_google import LOGGER, authenticate, configure_logging, FileMetadataDownloader


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Download and save metadata for drive files that '
                                        'produced errors last time, writing to new file')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('input_path', help='Path to input jsonl file')
    parser.add_argument('output_path', help='Path to output jsonl file')
    parser.add_argument('--sleep', type=float,
                        help='Amount of time to sleep between files')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('redownload-drive-metadata-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    with open(args.input_path) as input_file, open(args.output_path, 'w') as output_file:
        downloader = FileMetadataDownloader(files, perms)
        page_token = None
        batch_info = None

        LOGGER.info('Looking for files with errors ...')
        for line in input_file:
            metadata = json.loads(line)

            old_batch_info = batch_info
            batch_info = metadata['batch_info']
            if old_batch_info is not None and (
                    batch_info['next_page_token'] !=
                    old_batch_info['next_page_token']):
                page_token = old_batch_info['next_page_token']

            if metadata['error']:
                LOGGER.info('Redownloading metadata for {}'.format(metadata['name']))
                f = files.get(
                    fileId=metadata['id'],
                    fields=', '.join(FileMetadataDownloader.DEFAULT_FILE_FIELDS)).execute()
                metadata = downloader.get(f, metadata['batch_info'])
                if args.sleep:
                    time.sleep(args.sleep)

            output_file.write(json.dumps(metadata) + '\n')

        if batch_info is not None:
            if batch_info['item_index'] + 1 == batch_info['num_items']:
                page_token = batch_info['next_page_token']
                skip = 0
            elif page_token is not None:
                skip = batch_info['item_index'] + 1
            else:
                raise Exception('Previous pagination stopped in middle of batch and '
                                'we have no page token')

            if page_token is not None:
                LOGGER.info('Continuing pagination')
                for metadata in downloader.list(page_token=page_token):
                    if skip > 0:
                        skip -= 1
                    else:
                        output_file.write(json.dumps(metadata) + '\n')
                        if args.sleep:
                            time.sleep(args.sleep)


if __name__ == '__main__':
    main()
