#!/usr/bin/env python3

import json

from migrate_google import LOGGER, configure_logging, DriveFiles


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Load and compare metadata from drive files')
    parser.add_argument('old_path', help='Path to jsonl metadata for old drive')
    parser.add_argument('new_path', help='Path to jsonl metadata for new drive')
    args = parser.parse_args()

    configure_logging('compare-drive-metadata.log')

    drive_files = dict(old=DriveFiles(), new=DriveFiles())
    for (version, path) in (('old', args.old_path), ('new', args.new_path)):
        LOGGER.info('Loading {} data from {}'.format(version, path))
        with open(path) as f:
            for line in f:
                drive_files[version].add(json.loads(line))

    metadata_map = dict()
    for version in ('old', 'new'):
        for df in drive_files[version].list():
            key = (df.name, df.size, df.md5_checksum)
            if key not in metadata_map:
                metadata_map[key] = dict(old=[], new=[])
            metadata_map[key][version].append(df)

    LOGGER.info('Looking for files in old drive but not in new')
    for key in sorted(metadata_map, key=lambda k: k[1], reverse=True):
        copies_map = metadata_map[key]
        if len(copies_map['old']) > len(copies_map['new']):
            example_df = copies_map['old'][0]
            LOGGER.info('{:<8}: {:<20}: {:>3} x old, {:>3} x new'.format(
                example_df.human_friendly_size,
                example_df.name,
                len(copies_map['old']),
                len(copies_map['new'])))


if __name__ == '__main__':
    main()
