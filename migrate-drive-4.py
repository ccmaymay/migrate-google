#!/usr/bin/env python3

import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, service_method_iter, configure_logging


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Remove unshared orphaned files')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('email', help='Email address whose unshared files will be deleted')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('migrate-drive-4-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    LOGGER.debug('Searching for files owned by {} ...'.format(args.email))
    file_request = files.list(
        q="'{}' in owners and not mimeType contains 'application/vnd.google-apps'".format(args.email),
        pageSize=100,
        fields="nextPageToken, files(id, name, owners, parents)")
    for (f, _1) in service_method_iter(file_request, 'files', files.list_next):
        if not f.get('parents'):
            try:
                perm_request = perms.list(
                    fileId=f['id'], pageSize=10,
                    fields="nextPageToken, permissions(id, type, emailAddress)")
                for (p, _2) in service_method_iter(perm_request, 'permissions', perms.list_next):
                    if p['type'] != 'user' or p['emailAddress'] != args.email:
                        break
                else:
                    LOGGER.info('Removing orphaned file {}'.format(f['name']))
                    files.delete(fileId=f['id']).execute()

            except HttpError as ex:
                LOGGER.warning('Caught exception: {}'.format(ex))


if __name__ == '__main__':
    main()
