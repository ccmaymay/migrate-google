#!/usr/bin/env python3

import os
import json
import time

from googleapiclient.discovery import build

from migrate_google import authenticate, configure_logging, FileMetadataDownloader


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Download and save metadata from drive files')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('output_path', help='Path to output jsonl file')
    parser.add_argument('--sleep', type=float,
                        help='Amount of time to sleep between files')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('download-drive-metadata-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    with open(args.output_path, 'w') as output_file:
        downloader = FileMetadataDownloader(files, perms)
        for metadata in downloader.list():
            output_file.write(json.dumps(metadata) + '\n')
            if args.sleep:
                time.sleep(args.sleep)


if __name__ == '__main__':
    main()
