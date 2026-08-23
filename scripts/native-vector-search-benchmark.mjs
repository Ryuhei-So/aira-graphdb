#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { lstat, readFile, readdir, realpath, writeFile } from 'node:fs/promises';
import { createReadStream, existsSync, readFileSync } from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
import { basename, dirname, resolve } from 'node:path';

const DEFAULT_DIMENSIONS = 1024;
const DEFAULT_REPETITIONS = 3;
const REQUEST_TIMEOUT_MS = 300_000;
const SAMPLE_INTERVAL_MS = 20;

function usage(message) {
  if (message) console.error(`error: ${message}`);
  console.error([
    'usage: native-vector-search-benchmark.mjs',
    '  --db COPIED_SNAPSHOT_JSON --old OLD_BINARY --new NEW_BINARY',
    '  --out ARTIFACT_JSON [--old-sha SHA] [--new-sha SHA]',
    '  [--repetitions N] [--corpus ID] [--namespace NAME] [--top-k N]',
  ].join('\n'));
  process.exit(2);
}

function parseArgs(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith('--')) usage(`unknown argument ${arg}`);
    const key = arg.slice(2).replaceAll('-', '_');
    if (key === 'help') usage();
    const value = argv[index + 1];
    if (!value || value.startsWith('--')) usage(`missing value for --${key.replaceAll('_', '-')}`);
    values[key] = value;
    index += 1;
  }
  for (const required of ['db', 'old', 'new', 'out']) {
    if (!values[required]) usage(`missing --${required}`);
  }
  return values;
}

function sha256Bytes(bytes) {
  return createHash('sha256').update(bytes).digest('hex');
}

async function fileDigest(path, canonicalIdentities) {
  const [metadata, resolvedPath] = await Promise.all([lstat(path), realpath(path)]);
  if (!metadata.isFile() || metadata.nlink !== 1) {
    throw new Error(`benchmark snapshot file must be a private regular inode: ${path}`);
  }
  const identity = `${metadata.dev}:${metadata.ino}`;
  if (canonicalIdentities.has(identity)) {
    throw new Error(`refusing canonical/aliased inode in benchmark snapshot: ${path}`);
  }
  const digest = createHash('sha256');
  for await (const chunk of createReadStream(path)) digest.update(chunk);
  return { path, realpath: resolvedPath, bytes: metadata.size, sha256: digest.digest('hex'), device: metadata.dev, inode: metadata.ino };
}

function percentile(values, probability) {
  const sorted = [...values].sort((left, right) => left - right);
  if (sorted.length === 0) return null;
  const index = Math.min(sorted.length - 1, Math.ceil(sorted.length * probability) - 1);
  return sorted[index];
}

function parseStatus(status, name) {
  const line = status.split('\n').find((entry) => entry.startsWith(`${name}:`));
  return line ? Number(line.split(/\s+/).at(-2)) * 1024 : null;
}

function readMemory(pid) {
  try {
    const status = readFileSync(`/proc/${pid}/status`, 'utf8');
    const smaps = readFileSync(`/proc/${pid}/smaps_rollup`, 'utf8');
    return {
      rssBytes: parseStatus(status, 'VmRSS'),
      swapBytes: parseStatus(status, 'VmSwap'),
      pssBytes: parseStatus(smaps, 'Pss'),
    };
  } catch {
    return null;
  }
}

function maxMemory(samples) {
  return ['rssBytes', 'pssBytes', 'swapBytes'].reduce((result, key) => {
    result[key] = Math.max(0, ...samples.map((sample) => sample?.[key] ?? 0));
    return result;
  }, {});
}

function killGroup(child) {
  if (child.pid == null) return;
  try {
    process.kill(-child.pid, 'SIGKILL');
  } catch {
    child.kill('SIGKILL');
  }
}

function resultParity(response) {
  if (!response?.ok || !Array.isArray(response.result)) return { ok: false, hits: [] };
  return {
    ok: true,
    hits: response.result.map((hit) => ({
      id: hit.id,
      scoreBits: Buffer.from(new Float64Array([Number(hit.score)]).buffer).toString('hex'),
    })),
  };
}

function runRequest(child, request) {
  return new Promise((resolvePromise, reject) => {
    let buffer = '';
    let settled = false;
    const cleanup = () => {
      child.stdout.off('data', onData);
      child.off('error', onError);
      child.off('close', onClose);
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      cleanup();
      reject(error);
    };
    const onError = (error) => fail(error);
    const onClose = (code, signal) => fail(new Error(`native exited before response (code=${code}, signal=${signal})`));
    const timeout = setTimeout(() => {
      if (!settled) {
        settled = true;
        cleanup();
        reject(new Error(`request timeout after ${REQUEST_TIMEOUT_MS}ms`));
      }
    }, REQUEST_TIMEOUT_MS);
    const onData = (chunk) => {
      buffer += chunk;
      const newline = buffer.indexOf('\n');
      if (newline < 0 || settled) return;
      const line = buffer.slice(0, newline);
      settled = true;
      clearTimeout(timeout);
      cleanup();
      try {
        resolvePromise(JSON.parse(line));
      } catch (error) {
        reject(new Error(`invalid native response: ${error.message}`));
      }
    };
    child.stdout.on('data', onData);
    child.once('error', onError);
    child.once('close', onClose);
    child.stdin.write(`${JSON.stringify(request)}\n`);
  });
}

async function runBinary(binary, request, repetitions) {
  const samples = [];
  const coldMs = [];
  const warmMs = [];
  const parity = [];
  for (let repetition = 0; repetition < repetitions; repetition += 1) {
    const child = spawn(binary, ['--db', request.db], {
      detached: true,
      stdio: ['pipe', 'pipe', 'ignore'],
    });
    const initialMemory = readMemory(child.pid);
    if (initialMemory) samples.push(initialMemory);
    const poll = setInterval(() => {
      const memory = readMemory(child.pid);
      if (memory) samples.push(memory);
    }, SAMPLE_INTERVAL_MS);
    try {
      const coldStart = process.hrtime.bigint();
      const cold = await runRequest(child, { ...request.rpc, id: repetition * 2 + 1 });
      coldMs.push(Number(process.hrtime.bigint() - coldStart) / 1e6);
      const warmStart = process.hrtime.bigint();
      const warm = await runRequest(child, { ...request.rpc, id: repetition * 2 + 2 });
      warmMs.push(Number(process.hrtime.bigint() - warmStart) / 1e6);
      parity.push({ cold: resultParity(cold), warm: resultParity(warm) });
      child.stdin.end();
      await new Promise((resolvePromise, reject) => {
        child.once('close', resolvePromise);
        child.once('error', reject);
      });
    } finally {
      clearInterval(poll);
      killGroup(child);
    }
  }
  return {
    binary: resolve(binary),
    binarySha256: sha256Bytes(readFileSync(binary)),
    repetitions,
    coldMs,
    warmMs,
    p50Ms: { cold: percentile(coldMs, 0.5), warm: percentile(warmMs, 0.5) },
    p95Ms: { cold: percentile(coldMs, 0.95), warm: percentile(warmMs, 0.95) },
    peakMemory: maxMemory(samples),
    parity,
  };
}

function gitSha(path) {
  const result = spawnSync('git', ['-C', dirname(path), 'rev-parse', 'HEAD'], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : null;
}

const args = parseArgs(process.argv.slice(2));
const db = resolve(args.db);
const canonical = process.env.LITERATURE_HUB_CANONICAL_DB && resolve(process.env.LITERATURE_HUB_CANONICAL_DB);
if (canonical && db === canonical) usage('refusing to benchmark the configured canonical DB; pass a copied snapshot');
if (db.includes('/literature-hub/semantic/data/')) usage('refusing a Literature Hub live-data path; pass a copied snapshot');
if (!existsSync(db)) usage(`snapshot does not exist: ${db}`);

const canonicalIdentities = new Set();
if (canonical) {
  for (const entry of await readdir(dirname(canonical))) {
    try {
      const metadata = await lstat(resolve(dirname(canonical), entry));
      if (metadata.isFile()) canonicalIdentities.add(`${metadata.dev}:${metadata.ino}`);
    } catch {
      // The canonical directory may contain files which disappear during rotation.
    }
  }
}
const state = existsSync(`${db}.vblob`) ? {} : JSON.parse(await readFile(db, 'utf8'));
const snapshotPaths = [db];
if (state.vectorBlob?.basename) snapshotPaths.push(resolve(dirname(db), state.vectorBlob.basename));
else if (existsSync(`${db}.vblob`)) snapshotPaths.push(`${db}.vblob`);
const snapshotFiles = [];
for (const path of snapshotPaths) snapshotFiles.push(await fileDigest(path, canonicalIdentities));
const snapshotHash = sha256Bytes(snapshotFiles.map((file) => `${basename(file.path)}:${file.sha256}\n`).join(''));

const dimensions = Number(args.dimensions ?? DEFAULT_DIMENSIONS);
const repetitions = Number(args.repetitions ?? DEFAULT_REPETITIONS);
const queryVector = Array.from({ length: dimensions }, (_, index) => (index === 0 ? 1 : 0.001));
const rpc = {
  method: 'vector_search',
  params: {
    corpusId: args.corpus ?? 'libfull',
    namespace: args.namespace ?? 'fact',
    queryVector,
    topK: Number(args.top_k ?? 10),
  },
};
const request = { db, rpc };
const oldResult = await runBinary(args.old, request, repetitions);
const newResult = await runBinary(args.new, request, repetitions);
const parity = oldResult.parity.map((oldRun, index) => ({
  repetition: index + 1,
  coldEqual: JSON.stringify(oldRun.cold) === JSON.stringify(newResult.parity[index]?.cold),
  warmEqual: JSON.stringify(oldRun.warm) === JSON.stringify(newResult.parity[index]?.warm),
}));

const artifact = {
  schema: 'aira.native-vector-search-benchmark.v1',
  generatedAt: new Date().toISOString(),
  copiedSnapshotOnly: true,
  snapshot: { db, snapshotHash, files: snapshotFiles },
  rpc,
  repetitions,
  timingDefinition: {
    cold: 'fresh native process; elapsed from request write until response line',
    warm: 'same native process immediately after cold response; elapsed from request write until response line',
    p50: 'nearest-rank percentile over repetition wall-clock samples',
    p95: 'nearest-rank percentile over repetition wall-clock samples',
  },
  memoryDefinition: '20ms polling of /proc/$pid/status VmRSS/VmSwap and /proc/$pid/smaps_rollup Pss; peaks can under-sample short-lived maxima',
  binaries: { old: { ...oldResult, gitSha: args.old_sha ?? gitSha(args.old) }, new: { ...newResult, gitSha: args.new_sha ?? gitSha(args.new) } },
  parity,
};
await writeFile(resolve(args.out), `${JSON.stringify(artifact, null, 2)}\n`);
console.log(JSON.stringify({ out: resolve(args.out), snapshotHash, parity, old: artifact.binaries.old.p95Ms, new: artifact.binaries.new.p95Ms }));
