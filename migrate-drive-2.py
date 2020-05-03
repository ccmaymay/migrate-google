#!/usr/bin/env python3

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from migrate_google import LOGGER, authenticate, service_method_iter, configure_logging

LOG_PATH = 'migrate-drive-2.log'


class FileCache(object):
    def __init__(self, files):
        self.files = files
        self.file_id_map = {}

    def get(self, file_id):
        if file_id not in self.file_id_map:
            file_response = self.files.get(fileId=file_id, fields="owners").execute()
            self.file_id_map[file_id] = {
                'owned': any(owner['me'] for owner in file_response['owners']),
            }

        return self.file_id_map[file_id]

    def is_owned(self, file_id):
        return self.get(file_id)['owned']


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Copy files shared from an address and remove shared versions')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('email', help='Email address whose shared files will be copied')
    args = parser.parse_args()

    configure_logging(LOG_PATH)

    creds = authenticate(args.credentials_path)
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    LOGGER.debug('Searching for files owned by {} ...'.format(args.email))
    file_request = files.list(
        q="'{}' in owners".format(args.email), pageSize=100,
        fields="nextPageToken, files(id, name, description, starred, owners, mimeType, parents)")
    for f in service_method_iter(file_request, 'files', files.list_next):
        LOGGER.info(f['name'])
        if f['mimeType'] == 'application/vnd.google-apps.folder':
            LOGGER.warning('Skipping folder owned by {}'.format(args.email))
        else:
            copy_response = files.copy(
                fileId=f['id'], enforceSingleParent=True,
                fields='id',
                body=dict((k, f[k]) for k in ('name', 'description', 'starred'))).execute()
            LOGGER.debug('Copied file id: {}; deleting {}'.format(copy_response['id'], f['id']))
            files.delete(fileId=f['id'])


if __name__ == '__main__':
    main()
