#!/usr/bin/env python3

import logging
import pickle
import os
from humanfriendly import format_size

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
    DEFAULT_FILE_FIELDS = ('id', 'name', 'parents', 'size', 'mimeType', 'trashed', 'md5Checksum')
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
        metadata['error'] = None
        metadata['batch_info'] = batch_info
        try:
            perm_request = self.perms.list(
                fileId=f['id'], pageSize=10,
                fields="nextPageToken, permissions({})".format(', '.join(self.perm_fields)))
            for (p, _) in service_method_iter(perm_request, 'permissions', self.perms.list_next):
                metadata['permissions'].append(dict((k, p.get(k)) for k in self.perm_fields))

        except HttpError as ex:
            LOGGER.warning('Caught exception: {}'.format(ex))
            metadata['error'] = ex.resp.status

        return metadata


class DriveFile(object):
    def __init__(self, metadata, drive_files):
        self.metadata = dict()
        self.drive_files = drive_files

        self.children = []
        self.size = 0
        self.path = None

        self.update(metadata)

    @property
    def id(self):
        return self.metadata['id']

    @property
    def error(self):
        return self.metadata.get('error')

    @property
    def mime_type(self):
        return self.metadata.get('mimeType')

    @property
    def md5_checksum(self):
        return self.metadata.get('md5Checksum')

    @property
    def trashed(self):
        return self.metadata.get('trashed')

    @property
    def permissions(self):
        if self.metadata.get('permissions'):
            return self.metadata['permissions']
        else:
            return []

    @property
    def human_friendly_size(self):
        return format_size(self.size)

    @property
    def name(self):
        if self.metadata.get('name') is not None:
            return self.metadata['name']
        else:
            return '[id={}]'.format(self.metadata['id'])

    @property
    def parent_ids(self):
        if self.metadata.get('parents') is not None:
            return self.metadata['parents']
        else:
            return []

    @property
    def parents(self):
        return [self.drive_files.get(parent_id) for parent_id in self.parent_ids]

    def add(self, child):
        self.children.append(child)
        self.update_size(child.size)

    def update_size(self, diff):
        self.size += diff

        for parent in self.parents:
            parent.update_size(diff)

    def update_path(self):
        parents = self.parents
        if len(parents) == 0:
            self.path = self.name
        elif len(parents) == 1:
            self.path = '{}/{}'.format(parents[0].path, self.name)
        else:
            self.path = '{{{}}}/{}'.format(','.join(p.path for p in parents), self.name)

        for child in self.children:
            child.update_path()

    def update(self, metadata):
        new_size = (0 if metadata.get('size') is None else int(metadata['size']))
        old_size = (0 if self.metadata.get('size') is None else int(self.metadata['size']))

        self.metadata.update(metadata)
        self.update_size(new_size - old_size)
        self.update_path()

    def __str__(self):
        return '{} ({})'.format(self.id, self.path)


class DriveFiles(object):
    def __init__(self):
        self.file_map = dict()
        self.root_ids = set()

    def add(self, metadata):
        f = self.get(metadata['id'])
        f.update(metadata)

        for p in f.parents:
            p.add(f)
            if f.id in self.root_ids:
                self.root_ids.remove(f.id)

        return f

    def get(self, file_id):
        if file_id not in self.file_map:
            self.file_map[file_id] = DriveFile(dict(id=file_id), self)
            self.root_ids.add(file_id)
        return self.file_map[file_id]

    def list(self):
        return self.file_map.values()
