#!/usr/bin/env python3

import os

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, service_method_iter, configure_logging


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Change owner of all entries in drive')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('from_email', help='Email address of current owner')
    parser.add_argument('to_email', help='Email address of new owner')
    args = parser.parse_args()

    credentials_name = os.path.splitext(os.path.basename(args.credentials_path))[0]
    configure_logging('migrate-drive-1-{}.log'.format(credentials_name))

    creds = authenticate(args.credentials_path, token_path=credentials_name + '.pickle')
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    LOGGER.debug('Searching for files owned by {} ...'.format(args.from_email))
    file_request = files.list(
        q="'{}' in owners and mimeType contains 'application/vnd.google-apps'".format(args.from_email),
        pageSize=100,
        fields="nextPageToken, files(id, name)")
    for (f, _1) in service_method_iter(file_request, 'files', files.list_next):
        LOGGER.info('Changing owner of {} to {}'.format(f['name'], args.to_email))
        try:
            perm_request = perms.list(
                fileId=f['id'], pageSize=10,
                fields="nextPageToken, permissions(id, type, emailAddress)")
            for (p, _2) in service_method_iter(perm_request, 'permissions', perms.list_next):
                if p['type'] == 'user' and p['emailAddress'] == args.to_email:
                    LOGGER.debug('Updating permission to owner for {}'.format(args.to_email))
                    perms.update(
                        fileId=f['id'], permissionId=p['id'],
                        transferOwnership=True,
                        body={'role': 'owner'},
                    ).execute()
                    break

            else:
                LOGGER.debug('Adding owner permission for {}'.format(args.to_email))
                perms.create(
                    fileId=f['id'],
                    transferOwnership=True,
                    enforceSingleParent=True,
                    body={'role': 'owner', 'type': 'user', 'emailAddress': args.to_email},
                ).execute()

        except HttpError as ex:
            LOGGER.warning('Caught exception: {}'.format(ex))


if __name__ == '__main__':
    main()
