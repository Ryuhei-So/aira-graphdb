import assert from 'node:assert/strict';
import { chmod, link, mkdir, mkdtemp, readFile, readdir, symlink, unlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { spawn } from 'node:child_process';
import { promisify } from 'node:util';
import test from 'node:test';
import { createHash } from 'node:crypto';

const exec = promisify(execFile);
const script = new URL('./native-vector-search-benchmark.mjs', import.meta.url).pathname;
const manifestScript = new URL('./native-build-manifest.mjs', import.meta.url).pathname;

async function fixture({ descriptor = true, descriptorSize, descriptorSha, generation = 1, format = 1, slow = false } = {}) {
  const directory = await mkdtemp(join(tmpdir(), 'aira-bench-test-'));
  const db = join(directory, 'snapshot.json');
  const blob = join(directory, 'snapshot.vblob');
  const oldDir = join(directory, 'old-repo');
  const newDir = join(directory, 'new-repo');
  await mkdir(oldDir); await mkdir(newDir);
  const old = join(oldDir, 'aira-graphdb-native');
  const newer = join(newDir, 'aira-graphdb-native');
  await writeFile(blob, 'vectors');
  const blobSha = createHash('sha256').update('vectors').digest('hex');
  const state = descriptor ? { generation, vectorBlob: { basename: 'snapshot.vblob', size: descriptorSize ?? 7, sha256: descriptorSha ?? blobSha, format } } : { generation: 0, vectors: {} };
  await writeFile(db, JSON.stringify(state));
  const native = slow ? '#!/bin/sh\nwhile IFS= read -r line; do sleep 30; printf \'{"ok":true,"result":[]}\\n\'; done\n' : '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":true,"result":[]}\\n\'; done\n';
  await writeFile(old, native); await writeFile(newer, `${native}# distinct build\n`);
  await chmod(old, 0o700); await chmod(newer, 0o700);
  for (const repo of [oldDir, newDir]) {
    await exec('git', ['init', '-q', repo]);
    await exec('git', ['-C', repo, 'config', 'user.email', 'test@example.invalid']);
    await exec('git', ['-C', repo, 'config', 'user.name', 'test']);
    await exec('git', ['-C', repo, 'add', '.']);
    await exec('git', ['-C', repo, 'commit', '-qm', 'fixture']);
  }
  const oldSha = (await exec('git', ['-C', oldDir, 'rev-parse', 'HEAD'])).stdout.trim();
  const newSha = (await exec('git', ['-C', newDir, 'rev-parse', 'HEAD'])).stdout.trim();
  const oldHash = createHash('sha256').update(await readFile(old)).digest('hex');
  const newHash = createHash('sha256').update(await readFile(newer)).digest('hex');
  await writeFile(`${old}.manifest.json`, JSON.stringify({ schema: 'aira.native-build-manifest.v1', sourceSha: oldSha, binarySha256: oldHash, cargoProfile: 'release', rustcVersion: 'rustc test', buildCommand: 'cargo build --release' }));
  await writeFile(`${newer}.manifest.json`, JSON.stringify({ schema: 'aira.native-build-manifest.v1', sourceSha: newSha, binarySha256: newHash, cargoProfile: 'release', rustcVersion: 'rustc test', buildCommand: 'cargo build --release' }));
  return { directory, db, blob, old, newer, sha: oldSha, oldSha, newSha };
}

async function run(args) {
  try {
    const result = await exec(process.execPath, [script, ...args], { encoding: 'utf8' });
    return { code: 0, stdout: result.stdout, stderr: result.stderr };
  } catch (error) {
    return { code: error.code, stdout: error.stdout ?? '', stderr: error.stderr ?? '' };
  }
}

async function runManifest(args) {
  try { await exec(process.execPath, [manifestScript, ...args], { encoding: 'utf8' }); return { code: 0 }; }
  catch (error) { return { code: error.code, stderr: error.stderr ?? '' }; }
}

function argsFor(fixtureData, extra = []) {
  return ['--db', fixtureData.db, '--blob', fixtureData.blob, '--old', fixtureData.old, '--new', fixtureData.newer, '--old-sha', fixtureData.oldSha, '--new-sha', fixtureData.newSha, ...extra];
}

test('requires explicit blob, pinned SHAs, even repetitions, and has no output-path option', async () => {
  const f = await fixture();
  assert.notEqual((await run(['--db', f.db, '--old', f.old, '--new', f.new, '--out', join(f.directory, 'x')])).code, 0);
  assert.notEqual((await run(argsFor(f, ['--repetitions', '3'])).code), 0);
});

test('build manifest generator owns fixed clean build and rejects dirty/wrong/collision inputs', async () => {
  const repo = await mkdtemp(join(tmpdir(), 'aira-build-repo-'));
  await mkdir(join(repo, 'src'));
  await writeFile(join(repo, 'Cargo.toml'), '[package]\nname="aira-graphdb"\nversion="0.1.0"\nedition="2021"\n[[bin]]\nname="aira-graphdb-native"\npath="src/main.rs"\n');
  await writeFile(join(repo, 'src/main.rs'), 'fn main() {}\n');
  await exec('cargo', ['generate-lockfile'], { cwd: repo });
  await exec('git', ['init', '-q', repo]);
  await exec('git', ['-C', repo, 'config', 'user.email', 'test@example.invalid']);
  await exec('git', ['-C', repo, 'config', 'user.name', 'test']);
  await exec('git', ['-C', repo, 'add', '.']);
  await exec('git', ['-C', repo, 'commit', '-qm', 'fixture']);
  const sha = (await exec('git', ['-C', repo, 'rev-parse', 'HEAD'])).stdout.trim();
  const destination = await mkdtemp(join(tmpdir(), 'aira-private-output-')); await chmod(destination, 0o700);
  const generated = await runManifest(['--repo', repo, '--source-sha', sha, '--destination-dir', destination]);
  assert.equal(generated.code, 0);
  assert.equal((await runManifest(['--repo', repo, '--source-sha', sha, '--destination-dir', destination])).code, 0);
  const published = (await readdir(destination)).filter((entry) => entry.startsWith('.build-result-'));
  assert.equal(published.length, 2);
  assert.equal((await readdir(destination)).some((entry) => entry.startsWith('.cargo-target-')), false);
  const foreign = join(destination, '.build-result-foreign-owner');
  await mkdir(foreign);
  await writeFile(join(foreign, 'sentinel'), 'owned elsewhere');
  assert.equal((await runManifest(['--repo', repo, '--source-sha', sha, '--destination-dir', destination])).code, 0);
  assert.equal(await readFile(join(foreign, 'sentinel'), 'utf8'), 'owned elsewhere');
  const dirty = await mkdtemp(join(tmpdir(), 'aira-build-dirty-')); await mkdir(join(dirty, 'src'));
  await writeFile(join(dirty, 'Cargo.toml'), '[package]\nname="aira-graphdb"\nversion="0.1.0"\nedition="2021"\n[[bin]]\nname="aira-graphdb-native"\npath="src/main.rs"\n'); await writeFile(join(dirty, 'src/main.rs'), 'fn main() {}\n');
  await exec('cargo', ['generate-lockfile'], { cwd: dirty });
  await exec('git', ['init', '-q', dirty]); await exec('git', ['-C', dirty, 'config', 'user.email', 'test@example.invalid']); await exec('git', ['-C', dirty, 'config', 'user.name', 'test']); await exec('git', ['-C', dirty, 'add', '.']); await exec('git', ['-C', dirty, 'commit', '-qm', 'fixture']);
  const dirtySha = (await exec('git', ['-C', dirty, 'rev-parse', 'HEAD'])).stdout.trim(); await writeFile(join(dirty, 'dirty.txt'), 'dirty'); const dirtyOut = join(dirty, 'out'); await mkdir(dirtyOut); await chmod(dirtyOut, 0o700);
  assert.notEqual((await runManifest(['--repo', dirty, '--source-sha', dirtySha, '--destination-dir', dirtyOut])).code, 0);
  assert.notEqual((await runManifest(['--repo', repo, '--source-sha', '0'.repeat(40), '--destination-dir', join(repo, 'other-out')])).code, 0);
});

test('build manifest SIGTERM reaps its process group and cleans only its owned directories', async () => {
  const repo = await mkdtemp(join(tmpdir(), 'aira-build-signal-'));
  await mkdir(join(repo, 'src'));
  await writeFile(join(repo, 'Cargo.toml'), '[package]\nname="aira-graphdb"\nversion="0.1.0"\nedition="2021"\nbuild="build.rs"\n[[bin]]\nname="aira-graphdb-native"\npath="src/main.rs"\n');
  await writeFile(join(repo, 'src/main.rs'), 'fn main() {}\n');
  await writeFile(join(repo, 'build.rs'), 'fn main() { std::fs::write("build-script.pid", std::process::id().to_string()).unwrap(); std::thread::sleep(std::time::Duration::from_secs(30)); }\n');
  await exec('cargo', ['generate-lockfile'], { cwd: repo });
  await exec('git', ['init', '-q', repo]);
  await exec('git', ['-C', repo, 'config', 'user.email', 'test@example.invalid']);
  await exec('git', ['-C', repo, 'config', 'user.name', 'test']);
  await exec('git', ['-C', repo, 'add', '.']);
  await exec('git', ['-C', repo, 'commit', '-qm', 'fixture']);
  const sha = (await exec('git', ['-C', repo, 'rev-parse', 'HEAD'])).stdout.trim();
  const destination = await mkdtemp(join(tmpdir(), 'aira-build-signal-output-'));
  await chmod(destination, 0o700);
  const foreign = join(destination, '.build-result-foreign-owner');
  await mkdir(foreign);
  await writeFile(join(foreign, 'sentinel'), 'owned elsewhere');
  const child = spawn(process.execPath, [manifestScript, '--repo', repo, '--source-sha', sha, '--destination-dir', destination], { stdio: ['ignore', 'pipe', 'pipe'] });
  const deadline = Date.now() + 10_000;
  let buildPid = null;
  while (Date.now() < deadline) {
    try { buildPid = Number((await readFile(join(repo, 'build-script.pid'), 'utf8')).trim()); break; }
    catch { await new Promise((resolvePromise) => setTimeout(resolvePromise, 25)); }
  }
  assert.ok(Number.isSafeInteger(buildPid));
  child.kill('SIGTERM');
  const exit = await Promise.race([
    new Promise((resolvePromise) => child.once('close', (code, signal) => resolvePromise({ code, signal }))),
    new Promise((_, rejectPromise) => setTimeout(() => rejectPromise(new Error('generator did not terminate')), 5000)),
  ]);
  assert.equal(exit.code, 143);
  await assert.rejects(readFile(`/proc/${buildPid}/status`, 'utf8'));
  assert.equal(await readFile(join(foreign, 'sentinel'), 'utf8'), 'owned elsewhere');
  assert.deepEqual((await readdir(destination)).sort(), ['.build-result-foreign-owner']);
});

test('SIGTERM kills child and removes owned temporary workspace', async () => {
  const f = await fixture({ slow: true });
  const child = spawn(process.execPath, [script, ...argsFor(f, ['--repetitions', '2', '--timeout-ms', '60000', '--startup-timeout-ms', '60000'])], { cwd: f.directory, env: { ...process.env, TMPDIR: f.directory }, stdio: ['ignore', 'pipe', 'pipe'] });
  const deadline = Date.now() + 5000;
  let workspaces = [];
  while (Date.now() < deadline) {
    workspaces = (await readdir(f.directory)).filter((entry) => entry.startsWith('aira-vector-benchmark-'));
    if (workspaces.length > 0) break;
    await new Promise((resolvePromise) => setTimeout(resolvePromise, 25));
  }
  assert.ok(workspaces.length > 0);
  child.kill('SIGTERM');
  const exit = await new Promise((resolvePromise) => child.once('close', (code, signal) => resolvePromise({ code, signal })));
  assert.equal(exit.code, 143);
  assert.equal((await readdir(f.directory)).filter((entry) => entry.startsWith('aira-vector-benchmark-')).length, 0);
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
  assert.notEqual((await run(argsFor(f, ['--overall-timeout-ms', '1']))).code, 0);
  assert.notEqual((await run(argsFor(await fixture({ generation: 0 }))).code), 0);
  assert.notEqual((await run(argsFor(await fixture({ format: 2 }))).code), 0);
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

  const hardBinary = join(f.old.slice(0, f.old.lastIndexOf('/')), 'old-hardlink-native');
  await link(f.old, hardBinary);
  await writeFile(`${hardBinary}.manifest.json`, await readFile(`${f.old}.manifest.json`));
  const hardBinaryRun = await run(argsFor({ ...f, old: hardBinary }, ['--repetitions', '2', '--sample-interval-ms', '0', '--timeout-ms', '1000', '--startup-timeout-ms', '1000']));
  assert.equal(hardBinaryRun.code, 0);

  await writeFile(f.old, '#!/bin/sh\nwhile IFS= read -r line; do printf \'{"ok":false,"error":{"code":"TEST_FAILURE","message":"deliberate"}}\\n\'; done\n');
  await chmod(f.old, 0o700);
  const failed = await run(argsFor(f, ['--repetitions', '2', '--sample-interval-ms', '0', '--timeout-ms', '1000', '--startup-timeout-ms', '1000']));
  assert.notEqual(failed.code, 0);
  assert.ok(JSON.parse(failed.stdout).failures.length > 0);
  assert.equal(failed.stdout.includes('/tmp/'), false);
});
