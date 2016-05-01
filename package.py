#!/usr/bin/python

import argparse
import requests
import json
import sys

URL = 'https://api.github.com/repos/rqlite/rqlite/releases'

class Parser(object):
    def __init__(self, text):
        self.text = text

    def parse(self):
        self.json = json.loads(self.text)

    def release_id(self, tag):
        for release in self.json:
            if release['tag_name'] == tag:
                return release['id']
        return None

    def upload_url(self, tag):
        for release in self.json:
            if release['tag_name'] == tag:
                return release['upload_url']
        return None

def parse_args():
    parser = argparse.ArgumentParser(description='Publish a rqlite release to GitHub.')
    parser.add_argument('tag', metavar='TAG', type=str,
                       help='tag in the form "vX.Y.Z"')
    parser.add_argument('token', metavar='TOKEN', type=str,
                       help='GitHub API token')

    return parser.parse_args()

def main():
    args = parse_args()

    r = requests.get(URL)
    if r.status_code != 200:
        print 'failed to download release information'
        sys.exit(1)

    p = Parser(r.text)
    p.parse()
    release_id = p.release_id(args.tag)
    if release_id == None:
        print 'unable to determine release ID for tag %s' % args.tag
        sys.exit(1)

if __name__ == '__main__':
    main()
