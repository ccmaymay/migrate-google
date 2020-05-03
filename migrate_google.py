#!/usr/bin/env python3

import logging
import pickle
import os

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

LOGGER = logging.getLogger(__name__)
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


def configure_logging(log_path):
    logging.getLogger().setLevel(logging.ERROR)
    LOGGER.setLevel(logging.DEBUG)
    add_handler(LOGGER, logging.StreamHandler(), level=logging.INFO)
    add_handler(LOGGER, logging.FileHandler(log_path, encoding='utf-8'), level=logging.DEBUG)


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
