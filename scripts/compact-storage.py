"""Compact the local SQLite database, preserve a compressed backup and compare every value."""
import hashlib
import json
import marshal
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import tarfile
import time

root = Path(__file__).resolve().parent.parent
os.chdir(root)
os.umask(0o077)
backup_dir = root / 'data/backups' / time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())
backup_dir.mkdir()
source_path = root / 'data/renfe.db'
before_path = backup_dir / 'before.db'
after_path = backup_dir / 'compacted.db'
report = {'started_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'tables': {}}

def readonly(path):
    return sqlite3.connect(f'file:{path}?mode=ro', uri=True)

def sha256(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def table_counts(db):
    names = [row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
    return {name: db.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0] for name in names}

subprocess.run(['systemctl', 'stop', 'retrasometro.service'], check=True)
pause_start = time.monotonic()
try:
    with sqlite3.connect(source_path) as source:
        source.execute('PRAGMA busy_timeout=30000')
        source.execute('PRAGMA synchronous=FULL')
        source.execute('PRAGMA cache_size=-262144')
        source.execute('PRAGMA temp_store=MEMORY')
        source.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        report['before_bytes'] = source_path.stat().st_size
        report['counts_before'] = table_counts(source)
        with sqlite3.connect(before_path) as before:
            source.backup(before)
        print(f'Backup coherente: {before_path}', flush=True)
        source.execute('DROP INDEX IF EXISTS idx_observations_train_time')
        source.execute('VACUUM')
        source.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        report['after_bytes'] = source_path.stat().st_size
        report['counts_after'] = table_counts(source)
        assert report['counts_before'] == report['counts_after'], 'Los recuentos cambiaron'
        with sqlite3.connect(after_path) as after:
            source.backup(after)
        print(f"Compactado: {report['before_bytes']} -> {report['after_bytes']} bytes; recuentos idénticos", flush=True)
finally:
    subprocess.run(['systemctl', 'start', 'retrasometro.service'], check=True)
    report['service_pause_seconds'] = round(time.monotonic() - pause_start, 2)
    print(f"Servicio reanudado; pausa {report['service_pause_seconds']} segundos", flush=True)

# Compare frozen snapshots after resuming ingestion: no later write can be lost or skew the comparison.
with readonly(before_path) as before, readonly(after_path) as after:
    assert before.execute('PRAGMA quick_check').fetchall() == [('ok',)]
    assert after.execute('PRAGMA quick_check').fetchall() == [('ok',)]
    for name, expected in report['counts_before'].items():
        columns = before.execute(f'PRAGMA table_info("{name}")').fetchall()
        primary = [row[1] for row in sorted(columns, key=lambda row: row[5]) if row[5]]
        order = ','.join(f'"{column}"' for column in primary) or 'rowid'
        query = f'SELECT * FROM "{name}" ORDER BY {order}'
        count = 0
        digest = hashlib.sha256()
        for original, compacted in zip(before.execute(query), after.execute(query), strict=True):
            encoded = marshal.dumps(original)
            assert encoded == marshal.dumps(compacted), f'Valor cambiado en {name}, fila {count}'
            digest.update(encoded)
            count += 1
            if count % 2000000 == 0:
                print(f'Verificando {name}: {count} filas', flush=True)
        assert count == expected
        report['tables'][name] = {'rows': count, 'sha256_values': digest.hexdigest()}
        print(f'Verificado {name}: {count} filas, todos los campos idénticos', flush=True)
    assert before.execute('SELECT * FROM sqlite_sequence ORDER BY name').fetchall() == after.execute('SELECT * FROM sqlite_sequence ORDER BY name').fetchall()
    chunks = before.execute('SELECT file_path,sha256 FROM observation_archive_chunks').fetchall()
    for file_path, expected_hash in chunks:
        assert sha256(Path(file_path)) == expected_hash, f'Archivo histórico cambiado: {file_path}'
    report['archive_chunks_verified'] = len(chunks)

package = backup_dir / 'backup.tar.zst'
report['sqlite_backup_sha256'] = sha256(before_path)
with package.open('xb') as output:
    compressor = subprocess.Popen(['zstd', '-q', '-T2', '-3', '-c'], stdin=subprocess.PIPE, stdout=output)
    try:
        with tarfile.open(fileobj=compressor.stdin, mode='w|') as archive:
            archive.add(before_path, arcname='data/renfe.db')
            for file_path, _ in chunks:
                path = Path(file_path).resolve()
                archive.add(path, arcname=str(path.relative_to(root)))
            archive.add(root / '.env', arcname='.env')
    finally:
        compressor.stdin.close()
    assert compressor.wait() == 0, 'Compresión fallida'
subprocess.run(['zstd', '-q', '-t', str(package)], check=True)
restored_path = backup_dir / 'restore-check.db'
decoder = subprocess.Popen(['zstd', '-q', '-d', '-c', str(package)], stdout=subprocess.PIPE)
restored_files = 0
archive_hashes = {str(Path(path).resolve().relative_to(root)):digest for path,digest in chunks}
with tarfile.open(fileobj=decoder.stdout, mode='r|') as archive:
    for entry in archive:
        if not entry.isfile():
            continue
        data = archive.extractfile(entry)
        if entry.name == 'data/renfe.db':
            with restored_path.open('xb') as output:
                shutil.copyfileobj(data, output)
        else:
            content_hash = hashlib.sha256()
            while block := data.read(1024 * 1024):
                content_hash.update(block)
            if entry.name != '.env':
                assert content_hash.hexdigest() == archive_hashes[entry.name]
                restored_files += 1
assert decoder.wait() == 0
assert restored_files == len(chunks)
assert sha256(restored_path) == report['sqlite_backup_sha256'], 'Restauración diferente del backup'
with readonly(restored_path) as restored:
    assert restored.execute('PRAGMA quick_check').fetchall() == [('ok',)]
    assert table_counts(restored) == report['counts_before']
report.update({'backup': str(package), 'backup_bytes': package.stat().st_size, 'backup_sha256': sha256(package), 'restore_verified': True})
(backup_dir / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
# Only remove this run's temporary snapshots once the complete compressed backup restores exactly.
for path in [before_path, after_path, restored_path]:
    path.unlink()
print(json.dumps(report, ensure_ascii=False), flush=True)
