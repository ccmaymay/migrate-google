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
    parser = ArgumentParser(description='Copy files shared from an address and remove shared versions')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('from_email', help='Email address whose shared files will be copied')
    parser.add_argument('to_email', help='Email address to which files will be copied')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('migrate-drive-3-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()
    parents_cache = FileCache(files)

    LOGGER.debug('Searching for files owned by {} ...'.format(args.from_email))
    file_request = files.list(
        q="'{}' in owners and not mimeType contains 'application/vnd.google-apps'".format(args.from_email),
        pageSize=100,
        fields="nextPageToken, files(id, name, starred, owners, parents)")
    for (f, _) in service_method_iter(file_request, 'files', files.list_next):
        try:
            if not all(parents_cache.is_owned(parent_id) for parent_id in f.get('parents', [])):
                LOGGER.warning('Skipping {} in folder owned by someone else'.format(f['name']))
            else:
                LOGGER.info('Copying {} and removing {}'.format(f['name'], args.from_email))
                copy_response = files.copy(
                    fileId=f['id'], enforceSingleParent=True,
                    fields='id',
                    body=dict((k, f[k]) for k in ('name', 'starred'))).execute()
                remove_user_permissions(perms, copy_response['id'], args.from_email)
                LOGGER.debug('Copied file id: {}; deleting {}'.format(copy_response['id'], f['id']))
                remove_user_permissions(perms, f['id'], args.to_email)

        except HttpError as ex:
            LOGGER.warning('Caught exception: {}'.format(ex))


if __name__ == '__main__':
    main()
