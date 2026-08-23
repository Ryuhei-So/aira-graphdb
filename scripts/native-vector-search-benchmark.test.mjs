import assert from 'node:assert/strict';
import { chmod, link, mkdtemp, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';
import { createHash } from 'node:crypto';

const exec = promisify(execFile);
const script = new URL('./native-vector-search-benchmark.mjs', import.meta.url).pathname;

async function fixture({ descriptor = true, descriptorSize, descriptorSha } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'aira-bench-test-'));
  const db = join(directory, 'snapshot.json');
  const blob = join(directory, 'snapshot.vblob');
  const old = join(directory, 'old-native');
  const newer = join(directory, 'new-native');
  await writeFile(blob, 'vectors');
  const blobSha = createHash('sha256').update('vectors').digest('hex');
  const state = descriptor ? { generation: 1, vectorBlob: { basename: 'snapshot.vblob', size: descriptorSize ?? 7, sha256: descriptorSha ?? blobSha, format: 1 } } : { generation: 0, vectors: {} };
  await writeFile(db, JSON.stringify(state));
  const native = '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":true,"result":[]}\\n\'; done\n';
  await writeFile(old, native); await writeFile(newer, native);
  await chmod(old, 0o700); await chmod(newer, 0o700);
  await exec('git', ['init', '-q', directory]);
  await exec('git', ['-C', directory, 'config', 'user.email', 'test@example.invalid']);
  await exec('git', ['-C', directory, 'config', 'user.name', 'test']);
  await exec('git', ['-C', directory, 'add', '.']);
  await exec('git', ['-C', directory, 'commit', '-qm', 'fixture']);
  const sha = (await exec('git', ['-C', directory, 'rev-parse', 'HEAD'])).stdout.trim();
  return { directory, db, blob, old, newer, sha };
}

async function run(args) {
  try {
    const result = await exec(process.execPath, [script, ...args], { encoding: 'utf8' });
    return { code: 0, stdout: result.stdout, stderr: result.stderr };
  } catch (error) {
    return { code: error.code, stdout: error.stdout ?? '', stderr: error.stderr ?? '' };
  }
}

function argsFor(fixtureData, extra = []) {
  return ['--db', fixtureData.db, '--blob', fixtureData.blob, '--old', fixtureData.old, '--new', fixtureData.newer, '--old-sha', fixtureData.sha, '--new-sha', fixtureData.sha, ...extra];
}

test('requires explicit blob, pinned SHAs, even repetitions, and has no output-path option', async () => {
  const f = await fixture();
  assert.notEqual((await run(['--db', f.db, '--old', f.old, '--new', f.new, '--out', join(f.directory, 'x')])).code, 0);
  assert.notEqual((await run(argsFor(f, ['--repetitions', '3'])).code), 0);
});

test('rejects symlink/hardlink sources and WAL before native execution', async () => {
  const f = await fixture();
  const linked = join(f.directory, 'linked.json');
  await symlink(f.db, linked);
  assert.notEqual((await run(argsFor({ ...f, db: linked }))).code, 0);
  const hard = join(f.directory, 'hard.json');
  await link(f.db, hard);
  assert.notEqual((await run(argsFor({ ...f, db: hard }))).code, 0);
  await writeFile(`${f.db.slice(0, -'.json'.length)}.agdb.wal`, 'pending');
  assert.notEqual((await run(argsFor(f))).code, 0);
});

test('rejects descriptor/blob mismatch and SHA mismatch', async () => {
  const sizeMismatch = await fixture({ descriptorSize: 6 });
  assert.notEqual((await run(argsFor(sizeMismatch))).code, 0);
  const hashMismatch = await fixture({ descriptorSha: '0'.repeat(64) });
  assert.notEqual((await run(argsFor(hashMismatch))).code, 0);
  const wrongName = await fixture();
  const wrong = join(wrongName.directory, 'wrong.vblob');
  await writeFile(wrong, 'wrong');
  assert.notEqual((await run(argsFor({ ...wrongName, blob: wrong }))).code, 0);
  assert.notEqual((await run(argsFor(wrongName, ['--old-sha', 'wrong']))).code, 0);
});

test('rejects legacy generations and unbounded request parameters', async () => {
  const legacy = await fixture({ descriptor: false });
  assert.notEqual((await run(argsFor(legacy))).code, 0);
  const f = await fixture();
  assert.notEqual((await run(argsFor(f, ['--dimensions', '999999999999999999999']))).code, 0);
  assert.notEqual((await run(argsFor(f, ['--top-k', '100000000']))).code, 0);
});

test('writes only stdout JSON, keeps sample-0 at zero, and reports paired failures nonzero', async () => {
  const f = await fixture();
  const result = await run(argsFor(f, ['--repetitions', '2', '--sample-interval-ms', '0', '--timeout-ms', '1000', '--startup-timeout-ms', '1000']));
  assert.equal(result.code, 0);
  const artifact = JSON.parse(result.stdout);
  assert.equal(artifact.sampling.sampleCount.old, 0);
  assert.equal(artifact.sampling.sampleCount.new, 0);
  assert.equal(artifact.sampledPhaseExecuted, false);
  assert.equal(Object.keys(artifact).includes('out'), false);

  const hardBinary = join(f.directory, 'old-hardlink-native');
  await link(f.old, hardBinary);
  const hardBinaryRun = await run(argsFor({ ...f, old: hardBinary }, ['--repetitions', '2', '--sample-interval-ms', '0', '--timeout-ms', '1000', '--startup-timeout-ms', '1000']));
  assert.equal(hardBinaryRun.code, 0);

  await writeFile(f.old, '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":false,"error":{"code":"TEST_FAILURE","message":"deliberate"}}\\n\'; done\n');
  await chmod(f.old, 0o700);
  const failed = await run(argsFor(f, ['--repetitions', '2', '--sample-interval-ms', '0', '--timeout-ms', '1000', '--startup-timeout-ms', '1000']));
  assert.notEqual(failed.code, 0);
  assert.ok(JSON.parse(failed.stdout).failures.length > 0);
});
