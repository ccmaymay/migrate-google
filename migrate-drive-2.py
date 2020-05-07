#!/usr/bin/env python3

import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import (
    LOGGER, authenticate, service_method_iter, configure_logging, FileCache,
    remove_user_permissions,
)


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Remove old email from files owned by new email')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('from_email', help='Email address to remove from shared files')
    parser.add_argument('to_email', help='Email address owning files to be updated')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('migrate-drive-2-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()
    parents_cache = FileCache(files)

    LOGGER.debug('Searching for files owned by {} and shared with {} ...'.format(
        args.to_email, args.from_email))
    file_request = files.list(
        q="'{}' in owners and '{}' in readers".format(args.to_email, args.from_email),
        pageSize=100,
        fields="nextPageToken, files(id, name)")
    for (f, _) in service_method_iter(file_request, 'files', files.list_next):
        try:
            if not all(parents_cache.is_owned(parent_id) for parent_id in f.get('parents', [])):
                LOGGER.warning('Skipping {} in folder owned by someone else'.format(f['name']))
            else:
                LOGGER.info('Removing {} from owned file {}'.format(args.from_email, f['name']))
                remove_user_permissions(perms, f['id'], args.from_email)

        except HttpError as ex:
            LOGGER.warning('Caught exception: {}'.format(ex))


if __name__ == '__main__':
    main()
