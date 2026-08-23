#!/usr/bin/env node
import { createHash } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, open } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

function fail(message) { throw new Error(message); }
function args(argv) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const key = argv[i].replace(/^--/, '').replaceAll('-', '_');
    const value = argv[++i];
    if (!value || value.startsWith('--')) fail(`missing --${key}`);
    out[key] = value;
  }
  for (const key of ['repo', 'binary', 'source_sha']) if (!out[key]) fail(`missing --${key.replaceAll('_', '-')}`);
  return out;
}

const input = args(process.argv.slice(2));
const repo = resolve(input.repo);
const binary = resolve(input.binary);
const head = spawnSync('git', ['-C', repo, 'rev-parse', 'HEAD'], { encoding: 'utf8' });
if (head.status !== 0 || head.stdout.trim() !== input.source_sha) fail('repo HEAD does not match --source-sha');
const dirty = spawnSync('git', ['-C', repo, 'status', '--porcelain'], { encoding: 'utf8' });
if (dirty.status !== 0 || dirty.stdout !== '') fail('repository is not clean');
const metadata = await lstat(binary);
if (metadata.isSymbolicLink() || !metadata.isFile() || metadata.nlink !== 1) fail('binary must be a private regular inode');
const source = await open(binary, constants.O_RDONLY | constants.O_NOFOLLOW);
const held = await source.stat();
if (held.dev !== metadata.dev || held.ino !== metadata.ino || held.nlink !== 1 || !held.isFile()) fail('binary changed during validation');
const digest = createHash('sha256');
for await (const chunk of source.createReadStream({ autoClose: false })) digest.update(chunk);
await source.close();
const manifest = {
  schema: 'aira.native-build-manifest.v1', sourceSha: input.source_sha,
  binarySha256: digest.digest('hex'), cargoProfile: input.cargo_profile ?? 'release',
  rustcVersion: input.rustc_version ?? 'unknown', buildCommand: input.build_command ?? 'cargo build --release',
};
const path = `${binary}.manifest.json`;
const output = await open(path, 'wx', 0o600);
try { await output.writeFile(`${JSON.stringify(manifest, null, 2)}\n`, 'utf8'); await output.sync(); }
finally { await output.close(); }
console.log(JSON.stringify(manifest));
