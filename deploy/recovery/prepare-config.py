#!/usr/bin/env python3
"""Prepare a new recovery configuration directory on the Docker host."""
import argparse
import hashlib
import os
from pathlib import Path
import secrets
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--ca-cert', type=Path, required=True)
    parser.add_argument('--server-cert', type=Path, required=True)
    parser.add_argument('--server-key', type=Path, required=True)
    args = parser.parse_args()
    target = args.directory
    if os.geteuid() != 0:
        parser.error('run as root to assign the two container UIDs')
    if not target.is_absolute() or target.exists() or target.is_symlink():
        parser.error('--directory must be a new absolute directory')
    subprocess.run(['openssl', 'verify', '-CAfile', str(args.ca_cert),
                    '-verify_hostname', 'clickhouse', str(args.server_cert)],
                   check=True, stdout=subprocess.DEVNULL)
    certificate_key = subprocess.check_output(
        ['openssl', 'x509', '-in', str(args.server_cert), '-pubkey', '-noout'])
    private_key = subprocess.check_output(
        ['openssl', 'pkey', '-in', str(args.server_key), '-pubout'])
    if certificate_key != private_key:
        parser.error('ClickHouse certificate and private key do not match')
    # mkdir is exclusive. A failure leaves only this new directory for inspection;
    # existing configuration and operator-provided keys are never changed.
    target.mkdir(mode=0o755)
    os.chmod(target, 0o755)
    template = Path(__file__).with_name('users.xml.template').read_text()
    for role in ('operator', 'backup', 'restore'):
        password = secrets.token_hex(32).encode()
        template = template.replace('@' + role.upper() + '_SHA256@',
                                    hashlib.sha256(password).hexdigest())
        write_file(target / (role + '.password'), password, 65532, 65532, 0o400)
    write_file(target / 'administrator.seed', secrets.token_hex(32).encode(),
               65532, 65532, 0o400)
    write_file(target / 'users.xml', template.encode(), 0, 0, 0o444)
    for source, name, uid, gid, mode in (
        (args.ca_cert, 'ca.crt', 0, 0, 0o444),
        (args.server_cert, 'server.crt', 0, 0, 0o444),
        (args.server_key, 'server.key', 101, 101, 0o400),
    ):
        write_file(target / name, source.read_bytes(), uid, gid, mode)
    print('Prepared recovery configuration at ' + str(target))


def write_file(path, data, uid, gid, mode):
    with path.open('xb') as output:
        os.chmod(path, 0o600)
        output.write(data)
    os.chown(path, uid, gid)
    os.chmod(path, mode)


if __name__ == '__main__':
    main()
