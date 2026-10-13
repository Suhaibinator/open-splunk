#!/bin/sh

set -eu
LC_ALL=C
export LC_ALL

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <development-env-file>" >&2
    exit 2
fi

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repository_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
environment_file=$1
case "$environment_file" in
    /*) ;;
    *) environment_file="$repository_root/$environment_file" ;;
esac
if [ ! -f "$environment_file" ]; then
    echo "development environment is missing; run 'make dev-clickhouse' first" >&2
    exit 1
fi

python3 - "$environment_file" "$repository_root" <<'PYTHON'
import os
from pathlib import Path
import sys

environment_file, repository_root = sys.argv[1:]
environment = dict(os.environ)
for raw in Path(environment_file).read_text().splitlines():
    line = raw.strip()
    if not line or line.startswith('#'):
        continue
    key, separator, value = line.partition('=')
    if not separator or not key.startswith('OPEN_SPLUNK_') or not key.replace('_', '').isalnum():
        raise SystemExit('Invalid development environment assignment')
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    environment[key] = value
state_root = Path(environment.get('OPEN_SPLUNK_DEVELOPMENT_STATE_ROOT', str(Path(repository_root) / 'data/development')))
export_root = Path(environment.get('OPEN_SPLUNK_DEVELOPMENT_EXPORT_ROOT', str(Path(repository_root) / 'exports/development')))
private_state, private_exports = state_root / 'private', export_root / 'private'
for directory in (state_root, private_state, export_root, private_exports):
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
binary = environment.get('OPEN_SPLUNK_DEVELOPMENT_SERVER_BINARY', str(Path(repository_root) / 'build/open-splunk-server'))
if not Path(binary).is_file() or not os.access(binary, os.X_OK):
    raise SystemExit("development server binary is missing; run 'make build-server'")
for key in ('POSTGRES_DSN', 'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET', 'PUBLIC_BASE_URL',
            'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_BUCKET',
            'CLICKHOUSE_PASSWORD', 'CLICKHOUSE_USERNAME'):
    if not environment.get('OPEN_SPLUNK_SERVER_' + key):
        raise SystemExit('Development environment requires OPEN_SPLUNK_SERVER_' + key)
port = environment.get('OPEN_SPLUNK_DEPLOY_HTTP_PORT', '8080')
if not port.isdigit() or not 1 <= int(port) <= 65535:
    raise SystemExit('Development HTTP port must be between 1 and 65535')
environment['OPEN_SPLUNK_SERVER_HTTP_LISTEN_ADDRESS'] = '127.0.0.1:' + port
environment['OPEN_SPLUNK_SERVER_MASTER_KEY_FILE'] = str(Path(private_state) / 'master.key')
environment['OPEN_SPLUNK_SERVER_LOCK_FILE'] = str(Path(private_state) / 'open-splunk-server-open_splunk.server.lock')
environment['OPEN_SPLUNK_SERVER_EXPORT_ARTIFACT_DIRECTORY'] = str(private_exports)
print('Open Splunk development server: http://localhost:' + port + '/signin/', flush=True)
os.execve(binary, [binary], environment)
PYTHON
