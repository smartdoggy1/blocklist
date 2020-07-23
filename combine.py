import argparse
import datetime
import hashlib
import io
import os
import re
import requests
import urllib.parse


combined_hosts = 'smartdoggy1_combined_hosts'
combined_everything_hosts = 'smartdoggy1_combined_hosts_including_backup'
combined_hosts_sources_dir = 'sources'
backup_hosts_sources = os.path.join('backup', 'sources.txt')
backup_hosts_destination_dir = os.path.join('backup', 'backup')
whitelist = 'whitelist'

parser = argparse.ArgumentParser()
parser.add_argument('-b', '--backup', action='store_true', help=f'Backup all lists listed in {backup_hosts_sources} to {backup_hosts_destination_dir}.')
parser.add_argument('-c', '--combine', action='store_true', help=f'Combine all files from {combined_hosts_sources_dir} to {combined_hosts}.')
parser.add_argument('-e', '--everything', action='store_true', help=f'When used with -c, include {backup_hosts_destination_dir} and store to {combined_everything_hosts}.')
parser.add_argument('-k', '--keep-old', action='store_true', help=f'When backing up, keep old domains that were removed in the newest version.')
parser.add_argument('-i', '--ignore-whitelist', action='store_true', help='When using -c, ignore applying the whitelist.')
args = parser.parse_args()

# blacklist these from being blacklisted
blacklist = {b'localhost', b'localhost.localdomain', b'broadcasthost', b'local'}
keep_regex = re.compile(b'([^#*<>]*)(#.*)?$')
ignore_regex = re.compile(b'([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3})$')


def load_file_to_set(opened_hostfile, data):
    '''Loads the hostfile (an opened file in binary mode, or BytesIO) to data. Stored as bytes.
       Returns the total lines read.
    '''
    c = 0
    for line in opened_hostfile:
        c += 1
        line = line.strip()
        match = keep_regex.match(line)
        if match:
            line, comments = match.groups()
            line = line.strip()
            # note: indexing a bytes object -> int
            if line == b'':
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
        lines = {line for line in f}
    longest = len(max(lines, key=len))
    for i, line in enumerate(sorted(lines)):
        line = line.strip()
        if not line:
            continue
        print(f'{i:2d} - {line:{longest}} - ', end='', flush=True)
        try:
            r = requests.get(line, stream=True, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.85 Safari/537.36'})
        except requests.exceptions.ConnectionError:
            print('offline')
            return
        r.raw.decode_content = True
        filepath = os.path.join(backup_hosts_destination_dir, f'{hashlib.sha256(line.encode()).hexdigest()[:8]}-{os.path.basename(urllib.parse.urlparse(line).path)}')
        # find differences
        old_data = set()
        new_data = set()
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                load_file_to_set(f, old_data)
        c = load_file_to_set(io.BytesIO(r.content), new_data)
        if args.keep_old:
            if len(new_data - old_data) == 0:
                print('nothing changed, skipping.')
                continue
            new_data = new_data.union(old_data)
        elif new_data == old_data:
            print('nothing changed, skipping.')
            continue
        with open(filepath, 'wb') as f:
            f.write(f'# {line}\n# Backed up on: {datetime.datetime.now().strftime("%Y-%m-%d")}\n\n'.encode())
            f.write(b'\n'.join(sorted(new_data)))
        print(f'wrote {len(new_data)}/{c} lines.')


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
    print(f'Written {len(data)}/{c} lines to {fname}.')


def main():
    if args.backup:
        backup()
    if args.combine:
        combine()


if __name__ == '__main__':
    main()
