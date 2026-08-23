#!/usr/bin/env node
import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { chmod, lstat, mkdir, mkdtemp, open, readdir, rename, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { basename, join, resolve } from 'node:path';

function fail(message) { throw new Error(message); }
function parse(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i].replace(/^--/, '').replaceAll('-', '_');
    const value = argv[++i];
    if (!value || value.startsWith('--')) fail(`missing --${key}`);
    out[key] = value;
  }
  for (const key of ['repo', 'source_sha', 'destination_dir']) if (!out[key]) fail(`missing --${key.replaceAll('_', '-')}`);
  return out;
}

const input = parse(process.argv.slice(2));
const repo = resolve(input.repo);
const destination = resolve(input.destination_dir);
const destinationStat = await lstat(destination);
if (destinationStat.isSymbolicLink() || !destinationStat.isDirectory() || (destinationStat.mode & 0o077) !== 0) fail('destination must be a private 0700 directory');
const head = spawnSync('git', ['-C', repo, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
if (head.status !== 0 || head.stdout.trim() !== input.source_sha) fail('repo HEAD does not match --source-sha');
const dirty = spawnSync('git', ['-C', repo, 'status', '--porcelain'], { encoding: 'utf8' });
if (dirty.status !== 0 || dirty.stdout !== '') fail('repository is not clean');
const freshTarget = await mkdtemp(join(destination, '.cargo-target-'));
await chmod(freshTarget, 0o700);
let published = false;
try {
const build = spawnSync('cargo', ['build', '--release', '--locked', '--bin', 'aira-graphdb-native'], { cwd: repo, env: { ...process.env, CARGO_TARGET_DIR: freshTarget }, encoding: 'utf8', stdio: 'pipe' });
if (build.status !== 0) fail(`fixed native build failed: ${build.stderr || build.stdout}`);
const postHead = spawnSync('git', ['-C', repo, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
const postDirty = spawnSync('git', ['-C', repo, 'status', '--porcelain'], { encoding: 'utf8' });
if (postHead.stdout.trim() !== input.source_sha || postDirty.stdout !== '') fail(`source checkout changed during build: ${postDirty.stdout}`);
const built = resolve(freshTarget, 'release/aira-graphdb-native');
const metadata = await lstat(built);
if (metadata.isSymbolicLink() || !metadata.isFile()) fail('fixed build did not produce a regular native binary');
const source = await open(built, constants.O_RDONLY | constants.O_NOFOLLOW);
const held = await source.stat();
if (held.dev !== metadata.dev || held.ino !== metadata.ino || !held.isFile()) fail('built binary changed during validation');
const staging = join(destination, `.staging-${randomUUID()}`);
await mkdir(staging, { mode: 0o700 });
const target = join(staging, basename(built));
const output = await open(target, 'wx', 0o700);
const digest = createHash('sha256');
try {
  for await (const chunk of source.createReadStream({ autoClose: false })) { digest.update(chunk); await output.write(chunk); }
  const after = await source.stat();
  if (after.dev !== held.dev || after.ino !== held.ino || after.size !== held.size || after.mtimeNs !== held.mtimeNs) fail('built binary changed while copying');
  await output.sync();
} finally { await source.close(); await output.close(); }
await chmod(target, 0o700);
const rustc = spawnSync('rustc', ['--version'], { encoding: 'utf8' });
const manifest = { schema: 'aira.native-build-manifest.v1', sourceSha: input.source_sha, binarySha256: digest.digest('hex'), cargoProfile: 'release', rustcVersion: rustc.stdout.trim(), buildCommand: 'cargo build --release --locked --bin aira-graphdb-native' };
const manifestHandle = await open(`${target}.manifest.json`, 'wx', 0o600);
try { await manifestHandle.writeFile(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8'); await manifestHandle.sync(); }
finally { await manifestHandle.close(); }
const dirHandle = await open(destination, constants.O_RDONLY | constants.O_DIRECTORY);
await dirHandle.sync(); await dirHandle.close();
const finalDir = join(destination, `build-${input.source_sha}`);
await rename(staging, finalDir);
published = true;
console.log(JSON.stringify({ binary: join(finalDir, basename(target)), manifest }));
} finally {
  await rm(freshTarget, { recursive: true, force: true });
  if (!published) {
    for (const entry of await readdir(destination)) if (entry.startsWith('.staging-')) await rm(join(destination, entry), { recursive: true, force: true });
  }
}
