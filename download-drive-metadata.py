#!/usr/bin/env python3

import os
import json

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, service_method_iter, configure_logging


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Download and save metadata from drive files')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('output_path', help='Path to output jsonl file')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('download-drive-metadata-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    with open(args.output_path, 'w') as output_file:
        LOGGER.debug('Listing files ...')
        file_fields = ('id', 'name', 'parents')
        perm_fields = ('id', 'type', 'role', 'emailAddress')
        file_request = files.list(
            pageSize=100,
            fields="nextPageToken, files({})".format(', '.join(file_fields)))
        for f in service_method_iter(file_request, 'files', files.list_next):
            LOGGER.info('Downloading metadata for {}'.format(f['name']))
            metadata = dict((k, f.get(k)) for k in file_fields)
            metadata['permissions'] = []
            metadata['error'] = False
            try:
                perm_request = perms.list(
                    fileId=f['id'], pageSize=10,
                    fields="nextPageToken, permissions({})".format(', '.join(perm_fields)))
                for p in service_method_iter(perm_request, 'permissions', perms.list_next):
                    metadata['permissions'].append(dict((k, p.get(k)) for k in perm_fields))

            except HttpError as ex:
                LOGGER.warning('Caught exception: {}'.format(ex))
                metadata['error'] = True

            output_file.write(json.dumps(metadata) + '\n')


if __name__ == '__main__':
    main()
