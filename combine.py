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
trim = 'trim'
hash_length = 8

parser = argparse.ArgumentParser(description='Allows the retrieval (--backup) of hosts files, and then merging all of the hosts files (--combine) to one single host file.')
backup_group = parser.add_argument_group('Backup-only Options')
combine_group = parser.add_argument_group('Combine-only Options')
search_group = parser.add_argument_group('Search-only Options')
backup_group.add_argument('-b', '--backup', action='store_true', help=f'Downloads all lists found in {backup_hosts_sources}, and store as a file in the directory {backup_hosts_destination_dir}.')
backup_group.add_argument('-k', '--keep-old', action='store_true', help=f'When using -b, keep old domains that were removed in the newest version.')
backup_group.add_argument('-s', '--select', nargs='+', help=f'When using -b, specify which {hash_length}-character hash(es) to back up (will check hashes against {backup_hosts_sources}).')
backup_group.add_argument('-i', '--ignore', nargs='+', help='The opposite of --select, that is, specify the hashes to exclude backing up.')
backup_group.add_argument('--clean', action='store_true', help=f'Formats each file in {backup_hosts_destination_dir}.')
combine_group.add_argument('-c', '--combine', action='store_true', help=f'Combine all files from {combined_hosts_sources_dir} to {combined_hosts}.')
combine_group.add_argument('-e', '--everything', action='store_true', help=f'When used with -c, include {backup_hosts_destination_dir} and store to {combined_everything_hosts}.')
combine_group.add_argument('-iw', '--ignore-whitelist', action='store_true', help='When using -c, ignore applying the whitelist.')
combine_group.add_argument('-t', '--trim', action='store_true', help=f'When using -c, exclude all hosts that match regexes in the file {trim}. Useful to lower the final file size.')
search_group.add_argument('-se', '--search', action='store_true', help='Interactively prompts for a host to search an exact match in the host file (--search-source-file).')
search_group.add_argument('-sf', '--search-file', help='Provide a file with newline-separated hosts. Outputs each host with either True or False per line, then exits.')
search_group.add_argument('-ssf', '--search-source-file', default=combined_everything_hosts, help=f'Provide the hosts file to search against. Default: {combined_everything_hosts}')
args = parser.parse_args()

if args.search and args.search_file:
    print('--search and --search-file are mutually exclusive.')
    exit()
elif args.select and args.ignore:
    print('--select and --ignore are mutually exclusive.')
    exit()

if args.select or args.ignore:
    for _hash in args.select or args.ignore:
        if len(_hash) != hash_length:
            print(f'Error - given hash ({_hash}) must be of length {hash_length}')
            exit()

# blacklist these from being blacklisted
blacklist = {b'localhost', b'localhost.localdomain', b'broadcasthost', b'local'}
keep_regex = re.compile(br'([^\#\*\<\>\:\/\\\{\}]*)(#.*)?$')
ignore_regex = re.compile(br'([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)$')
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
            if line == b'':
                continue
            elif line.startswith(b'127.0.0.1'):
                line = b'0.0.0.0' + line[9:]
            elif not line.startswith(b'0.0.0.0'):
                line = b'0.0.0.0\t' + line
            split = line.split()
            if len(split) == 2:
                if split[1] in blacklist or ignore_regex.match(split[1]) or b'.' not in split[1]:
                    continue
                data.add(split[0] + b'\t' + split[1].lower())
    return c


def clean():
    '''Use this when the filtering rules in load_file_to_set() has changed.'''
    target = backup_hosts_destination_dir
    print(f'Cleaning {target}...')
    for filename in os.listdir(target):
        filename = os.path.join(target, filename)
        data = set()
        with open(filename, 'rb') as f:
            # our commented header for each backup hostfile is 3 lines
            first_three_lines = f.readline() + f.readline() + f.readline()
            if not first_three_lines.startswith(b'#'):
                f.seek(0)
                first_three_lines = b''
            load_file_to_set(f, data)
        with open(filename, 'wb') as f:
            f.write(first_three_lines)
            f.write(b'\n'.join(sorted(data)))
    print(f'Cleaned all files in {target}.')


def backup():
    with open(backup_hosts_sources) as f:
        # sources is a dict of {hash: url}, skips commented lines
        sources = {hashlib.sha256(line.strip().encode()).hexdigest()[:hash_length]: line.strip() for line in f if not line.startswith('#')}
    if args.select:
        sources = {_hash: url for _hash, url in sources.items() if _hash in args.select}
    elif args.ignore:
        sources = {_hash: url for _hash, url in sources.items() if _hash not in args.ignore}
    if len(sources) == 0:
        if args.select:
            print(f'Warning - no hashes matched when using --select')
        else:
            print(f"Warning - check {backup_hosts_sources} to make sure it's not empty")
        return
    longest = max(len(source) for source in sources.values())
    # sort by url
    for i, _ in enumerate(sorted(sources.items(), key=lambda _tuple: _tuple[1])):
        _hash, url = _
        if not url:
            continue
        print(f'{i:2} - {_hash} - {url:{longest}} - ', end='', flush=True)
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
        raw_bytes = io.BytesIO(r.content)
        if raw_bytes.read(1) == b'<':
            print('warning: this seems to be an html file - ', end='', flush=True)
        raw_bytes.seek(0)
        load_file_to_set(raw_bytes, new_data)
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
        print(f'wrote {len(new_data):,} lines ({len(new_data - old_data):+,d}{f", -{removed:,}" if removed else ""})')


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


def apply_trim(data):
    with open(trim, 'rb') as f:
        regexes = [re.compile(line.strip()) for line in f if line.strip()]
    to_remove = set()
    for line in data:
        test_line = line[8:]
        for regex in regexes:
            if regex.search(test_line):
                to_remove.add(line)
                break
    data.difference_update(to_remove)
    return len(to_remove)


def combine():
    data = set()
    print(f'Merging {combined_hosts_sources_dir}...', end='', flush=True)
    c = store_hosts(data, combined_hosts_sources_dir)
    print(f'merged {c:,} entries')
    if args.everything:
        print(f'Merging {backup_hosts_destination_dir} too...', end='', flush=True)
        new_c = store_hosts(data, backup_hosts_destination_dir)
        c += new_c
        print(f'merged {new_c:,} extra entries')
    fname = combined_everything_hosts if args.everything else combined_hosts
    if not args.ignore_whitelist:
        print(f'Whitelisted {apply_whitelist(data)} entries.')
    if args.trim:
        print('Trimming...', end='', flush=True)
        print(f'trimmed {apply_trim(data):,} entries.')
    with open(fname, 'wb') as f:
        f.write(b'\n'.join(sorted(data)))
    print('Writing to disk...', flush=True)
    print(f'Written {len(data):,} lines to {fname} (removed {c - len(data):,} duplicates).')


def search_file():
    data = set()
    with open(args.search_source_file, 'rb') as f:
        load_file_to_set(f, data)
    try:
        with open(args.search_file, 'rb') as f:
            lines = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f'Error when opening "{args.search_file}": {e}')
    if not lines:
        return
    longest = len(max(lines, key=len))
    for line in lines:
        target = b'0.0.0.0\t' + line
        print(f'{line.decode():{longest}} - {target in data}')


def interactive_search():
    print('Constructing database...', end='', flush=True)
    import sqlite3
    con = sqlite3.connect(':memory:')
    cur = con.cursor()
    cur.execute('CREATE TABLE hosts (host BLOB, PRIMARY KEY(host))')
    with open(args.search_source_file, 'rb') as f:
        cur.executemany('INSERT INTO hosts VALUES(?)', ((line.strip()[8:],) for line in f if line.strip()))
    print('done')
    partial = False
    while True:
        inp = input(f'Enter a host to search {"partially" if partial else "exactly"} for (p={"exact search" if partial else "partial search"}, q=quit): ')
        if inp == 'q':
            break
        elif inp == 'p':
            partial ^= True
            continue
        inp = inp.encode()
        if partial:
            cur.execute(f'SELECT host FROM hosts WHERE host LIKE ?', (f'%{inp.decode()}%',))
            found = [row[0].decode() for row in cur.fetchall()]
            longest = len(str(len(found) + 1))
            for i, host in enumerate(found):
                print(f'{i + 1:{longest}}: {host}')
            print()
        else:
            cur.execute('SELECT host FROM hosts WHERE host=?', (inp,))
            print(bool(cur.fetchone()))


def main():
    if args.search_file:
        search_file()
        return
    elif args.search:
        interactive_search()
        return
    if args.backup:
        backup()
    if args.clean:
        clean()
    if args.combine:
        combine()


if __name__ == '__main__':
    main()
