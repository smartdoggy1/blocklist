import argparse
import datetime
import hashlib
import io
import os
import re
import requests
import urllib.parse
from string import whitespace


combined_hosts = 'smartdoggy1_combined_hosts'
combined_everything_hosts = 'smartdoggy1_combined_hosts_including_backup'
combined_hosts_sources_dir = 'sources'
backup_hosts_sources = os.path.join('backup', 'sources.txt')
backup_hosts_destination_dir = os.path.join('backup', 'backup')
whitelist = 'whitelist'
hash_length = 8

parser = argparse.ArgumentParser(description='Allows the retrieval (--backup) of hosts files, and then merging all of the hosts files (--combine) to one single host file.')
backup_group = parser.add_argument_group('Backup-only Options')
combine_group = parser.add_argument_group('Combine-only Options')
backup_group.add_argument('-b', '--backup', action='store_true', help=f'Downloads all lists found in {backup_hosts_sources}, and store as a file in the directory {backup_hosts_destination_dir}.')
backup_group.add_argument('-k', '--keep-old', action='store_true', help=f'When using -b, keep old domains that were removed in the newest version.')
backup_group.add_argument('-s', '--select', nargs='+', help=f'When using -b, specify which {hash_length}-character hash(es) to back up (will check hashes against {backup_hosts_sources}).')
combine_group.add_argument('-c', '--combine', action='store_true', help=f'Combine all files from {combined_hosts_sources_dir} to {combined_hosts}.')
combine_group.add_argument('-e', '--everything', action='store_true', help=f'When used with -c, include {backup_hosts_destination_dir} and store to {combined_everything_hosts}.')
combine_group.add_argument('-i', '--ignore-whitelist', action='store_true', help='When using -c, ignore applying the whitelist.')
args = parser.parse_args()
if args.select:
    for _hash in args.select:
        if len(_hash) != hash_length:
            print(f'Error - given hash ({_hash}) must be of length {hash_length}')
            exit()

# blacklist these from being blacklisted
blacklist = {b'localhost', b'localhost.localdomain', b'broadcasthost', b'local'}
keep_regex = re.compile(b'([^\#\*\<\>\:\/\\\{\}]*)(#.*)?$')
ignore_regex = re.compile(b'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')
strip_chars = f'{whitespace}/'.encode()


def load_file_to_set(opened_hostfile, data):
    '''Loads the hostfile (an opened file in binary mode, or BytesIO) to data. Stored as bytes.
       Returns the total lines read.
    '''
    c = 0
    for line in opened_hostfile:
        c += 1
        line = line.strip(strip_chars)
        match = keep_regex.match(line)
        if match:
            line, comments = match.groups()
            line = line.strip(strip_chars)
            # note: indexing a bytes object -> int
            if line == b'' or b'.' not in line:
                continue
            elif line.startswith(b'127.0.0.1'):
                line = b'0.0.0.0' + line[9:]
            elif not line.startswith(b'0.0.0.0'):
                line = b'0.0.0.0\t' + line
            split = line.split()
            if len(split) == 2:
                if split[1] in blacklist or ignore_regex.match(split[1]):
                    continue
                data.add(split[0] + b'\t' + split[1])
    return c


def backup():
    with open(backup_hosts_sources) as f:
        # sources is a dict of {hash: url}
        sources = {hashlib.sha256(line.strip().encode()).hexdigest()[:hash_length]: line.strip() for line in f}
    if args.select:
        sources = {_hash: url for _hash, url in sources.items() if _hash in args.select}
    if len(sources) == 0:
        if args.select:
            print(f'Warning - no hashes matched when using --select')
        else:
            print(f"Warning - check {backup_hosts_sources} to make sure it's not empty")
        return
    longest = len(max(sources.values(), key=len))
    # sort by url
    for i, _ in enumerate(sorted(sources.items(), key=lambda _tuple: _tuple[1])):
        _hash, url = _
        if not url:
            continue
        print(f'{i:2d} - {url:{longest}} - ', end='', flush=True)
        try:
            r = requests.get(url, stream=True, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'})
            if r.status_code >= 400:
                raise requests.exceptions.ConnectionError
            elif r.status_code >= 300:
                print(f'warning: got status code {r.status_code} - ', end='', flush=True)
        except requests.exceptions.ConnectionError:
            print('offline')
            continue
        r.raw.decode_content = True
        filepath = os.path.join(backup_hosts_destination_dir, f'{_hash}-{os.path.basename(urllib.parse.urlparse(url).path)}')
        # find differences
        old_data = set()
        new_data = set()
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                load_file_to_set(f, old_data)
        load_file_to_set(io.BytesIO(r.content), new_data)
        if args.keep_old:
            if len(new_data - old_data) == 0:
                print('nothing changed, skipping.')
                continue
            new_data = new_data.union(old_data)
        elif new_data == old_data:
            print('nothing changed, skipping.')
            continue
        with open(filepath, 'wb') as f:
            f.write(f'# {url}\n# Backed up on: {datetime.datetime.now().strftime("%Y-%m-%d")}\n\n'.encode())
            f.write(b'\n'.join(sorted(new_data)))
        removed = len(old_data - new_data)
        print(f'wrote {len(new_data):,} lines ({len(new_data - old_data):+,d}{f", -{removed:,}" if removed else ""}).')


def store_hosts(data, source_dir):
    c = 0
    for host in os.listdir(source_dir):
        with open(os.path.join(source_dir, host), 'rb') as f:
            c += load_file_to_set(f, data)
    return c


def apply_whitelist(data):
    w = set()
    with open(whitelist, 'rb') as f:
        load_file_to_set(f, w)
    before = len(data)
    data.difference_update(w)
    return before - len(data)


def combine():
    data = set()
    print(f'Merging {combined_hosts_sources_dir}...')
    c = store_hosts(data, combined_hosts_sources_dir)
    if args.everything:
        print(f'Merging {backup_hosts_destination_dir} too...')
        c += store_hosts(data, backup_hosts_destination_dir)
    fname = combined_everything_hosts if args.everything else combined_hosts
    if not args.ignore_whitelist:
        print(f'Whitelisted {apply_whitelist(data)} entries.')
    with open(fname, 'wb') as f:
        f.write(b'\n'.join(sorted(data)))
    print(f'Written {len(data):,}/{c:,} lines to {fname}.')


def main():
    if args.backup:
        backup()
    if args.combine:
        combine()


if __name__ == '__main__':
    main()
