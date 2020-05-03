#!/usr/bin/env python3

import pickle
import os

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']


def service_method_iter(request, response_key, service_method_next):
    while request is not None:
        response = request.execute()
        for item in response.get(response_key, []):
            yield item
        request = service_method_next(request, response)


def main():
    """Shows basic usage of the Drive v3 API.
    Prints the names and ids of the first 10 files the user has access to.
    """
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Change owner of all entries in drive')
    parser.add_argument('credentials_path', help='Path to credentials json file')
    parser.add_argument('from_email', help='Email address of current owner')
    parser.add_argument('to_email', help='Email address of new owner')
    args = parser.parse_args()

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                args.credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    service_files = service.files()
    service_permissions = service.permissions()

    # Call the Drive v3 API
    file_request = service_files.list(
        pageSize=100, fields="nextPageToken, files(id, name, owners)")
    for f in service_method_iter(file_request, 'files', service_files.list_next):
        print(f['name'], end='')
        if any(owner['emailAddress'] == args.from_email for owner in f['owners']):
            print(': {}'.format(args.from_email), end='')
            how_changed = None
            permission_request = service_permissions.list(
                fileId=f['id'], pageSize=10,
                fields="nextPageToken, permissions(id, type, role, emailAddress)")
            for p in service_method_iter(permission_request, 'permissions', service_permissions.list_next):
                if p['type'] == 'user' and p['emailAddress'] == args.to_email:
                    service_permissions.update(
                        fileId=f['id'], permissionId=p['id'],
                        transferOwnership=True,
                        body={'role': 'owner'},
                    ).execute()
                    how_changed = 'u'
                    break
            else:
                service_permissions.create(
                    fileId=f['id'],
                    transferOwnership=True,
                    enforceSingleParent=True,
                    body={'role': 'owner', 'type': 'user', 'emailAddress': args.to_email},
                ).execute()
                how_changed = 'c'
            print(' -{}-> {}'.format(how_changed, args.to_email), end='')

        print()


if __name__ == '__main__':
    main()
