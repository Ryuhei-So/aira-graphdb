import assert from 'node:assert/strict';
import { chmod, mkdtemp, readFile, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';

const exec = promisify(execFile);
const script = new URL('./native-vector-search-benchmark.mjs', import.meta.url);

async function fixture() {
  const directory = await mkdtemp(join(tmpdir(), 'aira-bench-test-'));
  const db = join(directory, 'snapshot.json');
  const binary = join(directory, 'fake-native');
  await writeFile(db, JSON.stringify({ generation: 0, vectors: {} }));
  await writeFile(binary, '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":true,"result":[]}\\n\'; done\n');
  await chmod(binary, 0o700);
  return { directory, db, binary };
}

async function run(args) {
  try {
    await exec(process.execPath, [script.pathname, ...args], { env: { ...process.env, LITERATURE_HUB_CANONICAL_DB: '' } });
    return { code: 0, stderr: '' };
  } catch (error) {
    return { code: error.code, stderr: `${error.stderr ?? ''}${error.stdout ?? ''}` };
  }
}

test('rejects a snapshot with adjacent legacy blob and conflicting generation descriptor', async () => {
  const { db, binary, directory } = await fixture();
  await writeFile(join(directory, 'snapshot.vblob'), 'legacy');
  await writeFile(db, JSON.stringify({ generation: 1, vectorBlob: { basename: 'other.vblob', size: 6, sha256: 'x', format: 1 } }));
  const result = await run(['--db', db, '--old', binary, '--new', binary, '--out', join(directory, 'out.json')]);
  assert.notEqual(result.code, 0);
  assert.match(result.stderr, /ambiguous vector blob/);
});

test('rejects WAL/recovery snapshots before spawning a native child', async () => {
  const { db, binary, directory } = await fixture();
  await writeFile(`${db.slice(0, -'.json'.length)}.agdb.wal`, 'pending');
  const result = await run(['--db', db, '--old', binary, '--new', binary, '--out', join(directory, 'out.json')]);
  assert.notEqual(result.code, 0);
  assert.match(result.stderr, /WAL\/recovery pending/);
});

test('rejects symlink output and records native ok=false as a failed run', async () => {
  const { db, binary, directory } = await fixture();
  const target = join(directory, 'target.json');
  const output = join(directory, 'out.json');
  await writeFile(target, 'sentinel');
  await symlink(target, output);
  const refused = await run(['--db', db, '--old', binary, '--new', binary, '--out', output]);
  assert.notEqual(refused.code, 0);
  assert.match(refused.stderr, /private regular inode/);

  await writeFile(binary, '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":false,"error":{"code":"TEST_FAILURE","message":"deliberate"}}\\n\'; done\n');
  await chmod(binary, 0o700);
  const safeOutput = join(directory, 'safe.json');
  const failed = await run(['--db', db, '--old', binary, '--new', binary, '--out', safeOutput, '--old-repetitions', '1', '--new-repetitions', '1']);
  assert.notEqual(failed.code, 0);
  const artifact = JSON.parse(await readFile(safeOutput, 'utf8'));
  assert.ok(artifact.failures.length > 0);
  assert.equal(artifact.failures[0].error.code, 'TEST_FAILURE');
});
