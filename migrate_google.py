#!/usr/bin/env python3

import logging
import pickle
import os

from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

LOGGER = logging.getLogger(__name__)
LOG_FORMAT = '%(asctime)-15s %(levelname)-8s %(message)s'

TOKEN_PATH = 'token.pickle'

SCOPES = ('https://www.googleapis.com/auth/drive',)


def service_method_iter(request, response_key, service_method_next):
    while request is not None:
        response = request.execute()
        items = response.get(response_key, [])
        for (i, item) in enumerate(items):
            yield (
                item,
                dict(next_page_token=response.get('nextPageToken'),
                     item_index=i,
                     num_items=len(items)),
            )
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


def configure_logging(log_path=None):
    logging.getLogger().setLevel(logging.ERROR)
    LOGGER.setLevel(logging.DEBUG)
    add_handler(LOGGER, logging.StreamHandler(), level=logging.INFO)
    if log_path is not None:
        add_handler(LOGGER, logging.FileHandler(log_path, encoding='utf-8'), level=logging.DEBUG)


class FileCache(object):
    def __init__(self, files):
        self.files = files
        self.file_id_map = {}

    def _get(self, file_id):
        if file_id not in self.file_id_map:
            file_response = self.files.get(fileId=file_id, fields="owners").execute()
            self.file_id_map[file_id] = any(owner['me'] for owner in file_response['owners'])

        return self.file_id_map[file_id]

    def is_owned(self, file_id):
        return self._get(file_id)


def remove_user_permissions(perms, file_id, email_address):
    perm_request = perms.list(
        fileId=file_id, pageSize=10,
        fields="nextPageToken, permissions(id, type, emailAddress)")
    for (p, _) in service_method_iter(perm_request, 'permissions', perms.list_next):
        if p['type'] == 'user' and p['emailAddress'] == email_address:
            LOGGER.debug('Removing permission for {}'.format(email_address))
            perms.delete(fileId=file_id, permissionId=p['id']).execute()


class FileMetadataDownloader(object):
    DEFAULT_FILE_FIELDS = ('id', 'name', 'parents', 'size', 'mimeType', 'trashed')
    DEFAULT_PERM_FIELDS = ('id', 'type', 'role', 'emailAddress')

    def __init__(self, files, perms, file_fields=DEFAULT_FILE_FIELDS,
                 perm_fields=DEFAULT_PERM_FIELDS):
        self.files = files
        self.perms = perms
        self.file_fields = file_fields
        self.perm_fields = perm_fields

    def list(self, page_token=None):
        LOGGER.debug('Listing files ...')
        file_request = self.files.list(
            pageToken=page_token,
            pageSize=100,
            fields="nextPageToken, files({})".format(', '.join(self.file_fields)))
        for (f, batch_info) in service_method_iter(file_request, 'files', self.files.list_next):
            LOGGER.info('Downloading metadata for {}'.format(f['name']))
            yield self.get(f, batch_info)

    def get(self, f, batch_info):
        metadata = dict((k, f.get(k)) for k in self.file_fields)
        metadata['permissions'] = []
        metadata['error'] = False
        metadata['batch_info'] = batch_info
        try:
            perm_request = self.perms.list(
                fileId=f['id'], pageSize=10,
                fields="nextPageToken, permissions({})".format(', '.join(self.perm_fields)))
            for (p, _) in service_method_iter(perm_request, 'permissions', self.perms.list_next):
                metadata['permissions'].append(dict((k, p.get(k)) for k in self.perm_fields))

        except HttpError as ex:
            LOGGER.warning('Caught exception: {}'.format(ex))
            metadata['error'] = True

        return metadata
