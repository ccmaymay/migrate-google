#!/usr/bin/env python3

import logging
import pickle
import os

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

LOGGER = logging.getLogger(__name__)
LOG_PATH = 'migrate-drive-1.log'
LOG_FORMAT = '%(asctime)-15s %(levelname)-8s %(message)s'

TOKEN_PATH = 'token.pickle'

SCOPES = ('https://www.googleapis.com/auth/drive',)


def service_method_iter(request, response_key, service_method_next):
    while request is not None:
        response = request.execute()
        for item in response.get(response_key, []):
            yield item
        request = service_method_next(request, response)


def add_handler(logger, handler, level=logging.INFO, fmt=LOG_FORMAT):
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)


def authenticate(credentials_path, scopes=SCOPES, token_path=TOKEN_PATH):
    creds = None

    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        LOGGER.debug('Reading token from {}'.format(token_path))
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            LOGGER.debug('Refreshing token')
            creds.refresh(Request())
        else:
            LOGGER.debug('Getting new token')
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, scopes)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        LOGGER.debug('Writing token to {}'.format(token_path))
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    return creds


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Change owner of all entries in drive')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('from_email', help='Email address of current owner')
    parser.add_argument('to_email', help='Email address of new owner')
    args = parser.parse_args()

    logging.getLogger().setLevel(logging.ERROR)
    LOGGER.setLevel(logging.DEBUG)
    add_handler(LOGGER, logging.StreamHandler(), level=logging.INFO)
    add_handler(LOGGER, logging.FileHandler(LOG_PATH), level=logging.DEBUG)

    creds = authenticate(args.credentials_path)
    service = build('drive', 'v3', credentials=creds)
    files = service.files()
    perms = service.permissions()

    LOGGER.debug('Searching for files owned by {} ...'.format(args.from_email))
    file_request = files.list(
        q="'{}' in owners".format(args.from_email), pageSize=100,
        fields="nextPageToken, files(id, name, owners)")
    for f in service_method_iter(file_request, 'files', files.list_next):
        LOGGER.info(f['name'])
        try:
            perm_request = perms.list(
                fileId=f['id'], pageSize=10,
                fields="nextPageToken, permissions(id, type, role, emailAddress)")
            for p in service_method_iter(perm_request, 'permissions', perms.list_next):
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
