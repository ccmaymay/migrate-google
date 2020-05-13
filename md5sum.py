#!/usr/bin/env python3

import os

from googleapiclient.discovery import build

from migrate_google import LOGGER, authenticate, walk, configure_logging


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Print md5sum(s) for file/directory')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('path', help='Path of file/directory to print md5sum(s) for')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('md5sum-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()

    LOGGER.debug('Printing md5sum(s) under {} ...'.format(args.path))
    for (root, dir_entries, file_entries) in walk(
            args.path, files, fields=('id', 'name', 'mimeType', 'md5Checksum')):
        for file_entry in file_entries:
            if file_entry.get('name') and file_entry.get('md5Checksum'):
                print('{}  {}'.format(file_entry['md5Checksum'], file_entry['name']))


if __name__ == '__main__':
    main()
