#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { chmod, lstat, mkdtemp, open, rm } from 'node:fs/promises';
import { spawn, spawnSync } from 'node:child_process';
import { basename, join, resolve } from 'node:path';

const BUILD_TIMEOUT_MS = 30 * 60 * 1000;
const MAX_BUILD_DIAGNOSTIC_BYTES = 1024 * 1024;
let activeBuild = null;
let terminationSignal = null;
let terminationEscalation = null;

function fail(message) { throw new Error(message); }

function parse(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i].replace(/^--/, '').replaceAll('-', '_');
    const value = argv[++i];
    if (!value || value.startsWith('--')) fail(`missing --${key}`);
    out[key] = value;
  }
  for (const key of ['repo', 'source_sha', 'destination_dir']) {
    if (!out[key]) fail(`missing --${key.replaceAll('_', '-')}`);
  }
  return out;
}

function killBuild(signal) {
  if (!activeBuild?.pid) return;
  try { process.kill(-activeBuild.pid, signal); } catch (error) {
    if (error.code !== 'ESRCH') throw error;
  }
}

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    terminationSignal = signal;
    killBuild('SIGTERM');
    terminationEscalation = setTimeout(() => killBuild('SIGKILL'), 2000);
  });
}

function appendDiagnostic(current, chunk) {
  if (current.length >= MAX_BUILD_DIAGNOSTIC_BYTES) return current;
  return `${current}${chunk}`.slice(0, MAX_BUILD_DIAGNOSTIC_BYTES);
}

async function buildNative(repo, freshTarget) {
  let stdout = '';
  let stderr = '';
  let timedOut = false;
  let timeoutEscalation = null;
  const child = spawn('cargo', ['build', '--release', '--locked', '--bin', 'aira-graphdb-native'], {
    cwd: repo,
    env: { ...process.env, CARGO_TARGET_DIR: freshTarget },
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  activeBuild = child;
  child.stdout.setEncoding('utf8');
  child.stderr.setEncoding('utf8');
  child.stdout.on('data', (chunk) => { stdout = appendDiagnostic(stdout, chunk); });
  child.stderr.on('data', (chunk) => { stderr = appendDiagnostic(stderr, chunk); });
  const timeout = setTimeout(() => {
    timedOut = true;
    killBuild('SIGTERM');
    timeoutEscalation = setTimeout(() => killBuild('SIGKILL'), 2000);
  }, BUILD_TIMEOUT_MS);
  const result = await new Promise((resolveBuild, rejectBuild) => {
    child.once('error', rejectBuild);
    child.once('close', (code, signal) => resolveBuild({ code, signal }));
  }).finally(() => {
    clearTimeout(timeout);
    clearTimeout(timeoutEscalation);
    clearTimeout(terminationEscalation);
    activeBuild = null;
  });
  if (terminationSignal) fail(`build terminated by ${terminationSignal}`);
  if (timedOut) fail('fixed native build exceeded its 30 minute deadline');
  if (result.code !== 0) {
    fail(`fixed native build failed: ${stderr || stdout || result.signal || result.code}`);
  }
}

async function main() {
  const input = parse(process.argv.slice(2));
  const repo = resolve(input.repo);
  const destination = resolve(input.destination_dir);
  const destinationStat = await lstat(destination);
  if (destinationStat.isSymbolicLink() || !destinationStat.isDirectory()
    || (destinationStat.mode & 0o077) !== 0) {
    fail('destination must be a private 0700 directory');
  }
  const head = spawnSync('git', ['-C', repo, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
  if (head.status !== 0 || head.stdout.trim() !== input.source_sha) {
    fail('repo HEAD does not match --source-sha');
  }
  const dirty = spawnSync('git', ['-C', repo, 'status', '--porcelain'], { encoding: 'utf8' });
  if (dirty.status !== 0 || dirty.stdout !== '') fail('repository is not clean');

  const freshTarget = await mkdtemp(join(destination, '.cargo-target-'));
  let resultDir = null;
  let published = false;
  try {
    await chmod(freshTarget, 0o700);
    resultDir = await mkdtemp(join(destination, '.build-result-'));
    await chmod(resultDir, 0o700);
    if (terminationSignal) fail(`build terminated by ${terminationSignal}`);
    await buildNative(repo, freshTarget);
    const postHead = spawnSync('git', ['-C', repo, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
    const postDirty = spawnSync('git', ['-C', repo, 'status', '--porcelain'], { encoding: 'utf8' });
    if (postHead.status !== 0 || postDirty.status !== 0
      || postHead.stdout.trim() !== input.source_sha || postDirty.stdout !== '') {
      fail(`source checkout changed during build: ${postDirty.stdout}`);
    }
    if (terminationSignal) fail(`build terminated by ${terminationSignal}`);

    const built = resolve(freshTarget, 'release/aira-graphdb-native');
    const metadata = await lstat(built);
    if (metadata.isSymbolicLink() || !metadata.isFile()) {
      fail('fixed build did not produce a regular native binary');
    }
    const source = await open(built, constants.O_RDONLY | constants.O_NOFOLLOW);
    const held = await source.stat();
    if (held.dev !== metadata.dev || held.ino !== metadata.ino || !held.isFile()) {
      await source.close();
      fail('built binary changed during validation');
    }
    const target = join(resultDir, basename(built));
    const output = await open(target, 'wx', 0o700);
    const digest = createHash('sha256');
    try {
      for await (const chunk of source.createReadStream({ autoClose: false })) {
        if (terminationSignal) fail(`build terminated by ${terminationSignal}`);
        digest.update(chunk);
        await output.write(chunk);
      }
      const after = await source.stat();
      if (after.dev !== held.dev || after.ino !== held.ino
        || after.size !== held.size || after.mtimeNs !== held.mtimeNs) {
        fail('built binary changed while copying');
      }
      await output.sync();
    } finally {
      await source.close();
      await output.close();
    }
    await chmod(target, 0o700);

    const rustc = spawnSync('rustc', ['--version'], { encoding: 'utf8' });
    if (rustc.status !== 0 || !rustc.stdout.trim()) fail('rustc version probe failed');
    const manifest = {
      schema: 'aira.native-build-manifest.v1',
      sourceSha: input.source_sha,
      binarySha256: digest.digest('hex'),
      cargoProfile: 'release',
      rustcVersion: rustc.stdout.trim(),
      buildCommand: 'cargo build --release --locked --bin aira-graphdb-native',
    };
    const manifestHandle = await open(`${target}.manifest.json`, 'wx', 0o600);
    try {
      await manifestHandle.writeFile(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
      await manifestHandle.sync();
    } finally {
      await manifestHandle.close();
    }
    if (terminationSignal) fail(`build terminated by ${terminationSignal}`);
    const resultDirHandle = await open(resultDir, constants.O_RDONLY | constants.O_DIRECTORY);
    await resultDirHandle.sync();
    await resultDirHandle.close();
    const destinationHandle = await open(destination, constants.O_RDONLY | constants.O_DIRECTORY);
    await destinationHandle.sync();
    await destinationHandle.close();

    // The unguessable directory is the capability. It becomes authoritative
    // only when this success token is emitted, so no shared pathname is ever
    // replaced and another invocation's files are never cleanup candidates.
    published = true;
    console.log(JSON.stringify({ binary: target, manifest }));
  } finally {
    await rm(freshTarget, { recursive: true, force: true });
    if (resultDir && !published) await rm(resultDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error.stack ?? error.message ?? String(error));
  process.exitCode = terminationSignal
    ? 128 + (terminationSignal === 'SIGINT' ? 2 : 15)
    : 1;
});
