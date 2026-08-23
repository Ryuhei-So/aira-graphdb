#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { lstat, open, readFile, readdir, realpath, rename, unlink } from 'node:fs/promises';
import { constants, existsSync, readFileSync } from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
import { basename, dirname, join, resolve } from 'node:path';
import { randomUUID } from 'node:crypto';

const DEFAULT_DIMENSIONS = 1024;
const DEFAULT_REPETITIONS = 3;
const REQUEST_TIMEOUT_MS = 300_000;
const DEFAULT_SAMPLE_INTERVAL_MS = 250;

function usage(message) {
  if (message) console.error(`error: ${message}`);
  console.error([
    'usage: native-vector-search-benchmark.mjs',
    '  --db COPIED_SNAPSHOT_JSON --old OLD_BINARY --new NEW_BINARY',
    '  --out ARTIFACT_JSON [--old-sha SHA] [--new-sha SHA]',
    '  [--repetitions N] [--old-repetitions N] [--new-repetitions N]',
    '  [--timeout-ms N] [--startup-timeout-ms N] [--sample-interval-ms N] [--preload]',
    '  [--corpus ID] [--namespace NAME] [--top-k N]',
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
    if (key === 'preload') {
      values[key] = 'true';
      continue;
    }
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
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const held = await handle.stat();
    if (held.dev !== metadata.dev || held.ino !== metadata.ino || held.nlink !== 1 || !held.isFile()) {
      throw new Error(`snapshot inode changed during validation: ${path}`);
    }
    const digest = createHash('sha256');
    for await (const chunk of handle.createReadStream({ autoClose: false })) digest.update(chunk);
    return { path, realpath: resolvedPath, bytes: held.size, sha256: digest.digest('hex'), device: held.dev, inode: held.ino };
  } finally {
    await handle.close();
  }
}

async function rejectUnsafePath(path, protectedIdentities, label) {
  const absolute = resolve(path);
  let metadata;
  try {
    metadata = await lstat(absolute);
  } catch (error) {
    if (error.code === 'ENOENT') return;
    throw error;
  }
  if (metadata.isSymbolicLink() || !metadata.isFile() || metadata.nlink !== 1) {
    throw new Error(`${label} must be a private regular inode: ${absolute}`);
  }
  if (protectedIdentities.has(`${metadata.dev}:${metadata.ino}`)) {
    throw new Error(`${label} aliases a protected snapshot/binary inode: ${absolute}`);
  }
}

async function writeArtifactAtomically(path, content, protectedIdentities) {
  const output = resolve(path);
  await rejectUnsafePath(output, protectedIdentities, 'benchmark output');
  const parent = resolve(dirname(output));
  const temp = join(parent, `.${basename(output)}.${process.pid}.${randomUUID()}.tmp`);
  const handle = await open(temp, 'wx', 0o600);
  try {
    await handle.writeFile(content, 'utf8');
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await rename(temp, output);
  } catch (error) {
    try { await unlink(temp); } catch {}
    throw error;
  }
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
  if (!response?.ok || !Array.isArray(response.result)) {
    return {
      ok: false,
      error: response?.error ?? { code: 'INVALID_NATIVE_RESPONSE', message: 'native response did not contain ok=true and result[]' },
      hits: [],
    };
  }
  return {
    ok: true,
    payloadSha256: sha256Bytes(JSON.stringify(response.result)),
    hits: response.result.map((hit) => ({
      id: hit.id,
      scoreBits: Buffer.from(new Float64Array([Number(hit.score)]).buffer).toString('hex'),
      metadataSha256: sha256Bytes(JSON.stringify(hit.metadata ?? null)),
    })),
  };
}

function assertRpcOk(response, method) {
  if (!response?.ok) {
    const error = response?.error && typeof response.error === 'object' ? response.error : { code: 'INVALID_NATIVE_RESPONSE', message: 'missing native error' };
    const failure = new Error(`${method} failed (${error.code ?? 'UNKNOWN'}): ${error.message ?? 'native returned ok=false'}`);
    failure.nativeError = { code: error.code ?? 'UNKNOWN', message: error.message ?? 'native returned ok=false' };
    throw failure;
  }
  return response;
}

function errorDetail(error) {
  if (error?.nativeError) return error.nativeError;
  return { code: 'BENCHMARK_ERROR', message: error instanceof Error ? error.message : String(error) };
}

function runRequest(child, request, timeoutMs) {
  return new Promise((resolvePromise, reject) => {
    let buffer = '';
    let settled = false;
    const cleanup = () => {
      child.stdout.off('data', onData);
      child.off('error', onError);
      child.off('close', onClose);
      child.stdin.off('error', onStdinError);
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
    const onStdinError = (error) => fail(error);
    const timeout = setTimeout(() => {
      if (!settled) {
        settled = true;
        cleanup();
        reject(new Error(`request timeout after ${timeoutMs}ms`));
      }
    }, timeoutMs);
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
    child.stdin.once('error', onStdinError);
    child.stdin.write(`${JSON.stringify(request)}\n`);
  });
}

async function runBinary(binary, request, repetitions, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs) {
  const samples = [];
  let sampleCount = 0;
  const preloadMs = [];
  const coldMs = [];
  const warmMs = [];
  const parity = [];
  for (let repetition = 0; repetition < repetitions; repetition += 1) {
    const child = spawn(binary, ['--db', request.db], {
      detached: true,
      stdio: ['pipe', 'pipe', 'ignore'],
    });
    const initialMemory = readMemory(child.pid);
    if (initialMemory) { samples.push(initialMemory); sampleCount += 1; }
    const poll = sampleIntervalMs > 0 ? setInterval(() => {
      const memory = readMemory(child.pid);
      if (memory) { samples.push(memory); sampleCount += 1; }
    }, sampleIntervalMs) : null;
    try {
      if (preload) {
        const preloadStart = process.hrtime.bigint();
        try {
          assertRpcOk(await runRequest(child, { id: repetition * 3 + 1, method: 'ping', params: {} }, startupTimeoutMs), 'ping');
          preloadMs.push(Number(process.hrtime.bigint() - preloadStart) / 1e6);
        } catch (error) {
          const detail = errorDetail(error);
          preloadMs.push(Number(process.hrtime.bigint() - preloadStart) / 1e6);
          parity.push({ preload: { ok: false, error: detail }, cold: null, warm: null });
          continue;
        }
      }
      const coldStart = process.hrtime.bigint();
      let cold;
      try {
        cold = assertRpcOk(await runRequest(child, { ...request.rpc, id: repetition * 3 + 2 }, timeoutMs), 'vector_search');
        coldMs.push(Number(process.hrtime.bigint() - coldStart) / 1e6);
      } catch (error) {
        const detail = errorDetail(error);
        coldMs.push(Number(process.hrtime.bigint() - coldStart) / 1e6);
        parity.push({ cold: { ok: false, error: detail }, warm: null });
        continue;
      }
      const coldParity = resultParity(cold);
      const warmStart = process.hrtime.bigint();
      try {
        const warm = assertRpcOk(await runRequest(child, { ...request.rpc, id: repetition * 3 + 3 }, timeoutMs), 'vector_search');
        warmMs.push(Number(process.hrtime.bigint() - warmStart) / 1e6);
        parity.push({ cold: coldParity, warm: resultParity(warm) });
      } catch (error) {
        const detail = errorDetail(error);
        warmMs.push(Number(process.hrtime.bigint() - warmStart) / 1e6);
        parity.push({ cold: coldParity, warm: { ok: false, error: detail } });
      }
      child.stdin.end();
      if (child.exitCode === null && child.signalCode === null) {
        await new Promise((resolvePromise) => child.once('close', resolvePromise));
      }
    } finally {
      if (poll) clearInterval(poll);
      killGroup(child);
    }
  }
  return {
    binary: resolve(binary),
    binarySha256: sha256Bytes(readFileSync(binary)),
    repetitions,
    preloadMs,
    coldMs,
    warmMs,
    p50Ms: { preload: percentile(preloadMs, 0.5), cold: percentile(coldMs, 0.5), warm: percentile(warmMs, 0.5) },
    p95Ms: { preload: percentile(preloadMs, 0.95), cold: percentile(coldMs, 0.95), warm: percentile(warmMs, 0.95) },
    timeoutMs,
    sampleCount,
    peakMemory: maxMemory(samples),
    parity,
  };
}

function combineRuns(runs) {
  const first = runs[0];
  const combined = {
    ...first,
    repetitions: runs.length,
    preloadMs: runs.flatMap((run) => run.preloadMs),
    coldMs: runs.flatMap((run) => run.coldMs),
    warmMs: runs.flatMap((run) => run.warmMs),
    sampleCount: runs.reduce((sum, run) => sum + run.sampleCount, 0),
    parity: runs.flatMap((run) => run.parity),
  };
  combined.p50Ms = { preload: percentile(combined.preloadMs, 0.5), cold: percentile(combined.coldMs, 0.5), warm: percentile(combined.warmMs, 0.5) };
  combined.p95Ms = { preload: percentile(combined.preloadMs, 0.95), cold: percentile(combined.coldMs, 0.95), warm: percentile(combined.warmMs, 0.95) };
  combined.peakMemory = ['rssBytes', 'pssBytes', 'swapBytes'].reduce((result, key) => {
    result[key] = Math.max(...runs.map((run) => run.peakMemory[key] ?? 0));
    return result;
  }, {});
  return combined;
}

async function runCounterbalanced(binaries, request, repetitions, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs) {
  const runs = { old: [], new: [] };
  const executionOrder = [];
  for (let repetition = 0; repetition < repetitions; repetition += 1) {
    const sequence = repetition % 2 === 0 ? ['old', 'new'] : ['new', 'old'];
    for (const label of sequence) {
      executionOrder.push({ repetition: repetition + 1, binary: label });
      runs[label].push(await runBinary(binaries[label], request, 1, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs));
    }
  }
  return { old: combineRuns(runs.old), new: combineRuns(runs.new), executionOrder };
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
const state = JSON.parse(await readFile(db, 'utf8'));
const adjacentBlob = db.endsWith('.json') ? `${db.slice(0, -'.json'.length)}.vblob` : `${db}.vblob`;
const descriptorBlob = state.vectorBlob?.basename ? resolve(dirname(db), state.vectorBlob.basename) : null;
if (descriptorBlob && dirname(descriptorBlob) !== dirname(db)) usage('vector blob descriptor must remain in the snapshot directory');
if (descriptorBlob && descriptorBlob !== adjacentBlob && existsSync(adjacentBlob)) {
  usage(`ambiguous vector blob: descriptor=${descriptorBlob} adjacent legacy=${adjacentBlob}`);
}
const walPath = `${db.endsWith('.json') ? db.slice(0, -'.json'.length) : db}.agdb.wal`;
if (existsSync(walPath)) usage(`refusing snapshot with WAL/recovery pending: ${walPath}`);
const recoveryEntries = (await readdir(dirname(db))).filter((entry) => entry.includes('.recovery-') || entry.endsWith('.quarantine'));
if (recoveryEntries.length > 0) usage(`refusing snapshot with recovery quarantine artifacts: ${recoveryEntries.join(', ')}`);
const snapshotPaths = [db];
if (descriptorBlob) snapshotPaths.push(descriptorBlob);
else if (existsSync(adjacentBlob)) snapshotPaths.push(adjacentBlob);
const snapshotFiles = [];
for (const path of snapshotPaths) snapshotFiles.push(await fileDigest(path, canonicalIdentities));
const snapshotHash = sha256Bytes(snapshotFiles.map((file) => `${basename(file.path)}:${file.sha256}\n`).join(''));
const protectedIdentities = new Set([...canonicalIdentities, ...snapshotFiles.map((file) => `${file.device}:${file.inode}`)]);
for (const binary of [args.old, args.new]) {
  const binaryFile = await fileDigest(binary, new Set());
  protectedIdentities.add(`${binaryFile.device}:${binaryFile.inode}`);
}

const dimensions = Number(args.dimensions ?? DEFAULT_DIMENSIONS);
const repetitions = Number(args.repetitions ?? DEFAULT_REPETITIONS);
const oldRepetitions = Number(args.old_repetitions ?? repetitions);
const newRepetitions = Number(args.new_repetitions ?? repetitions);
const timeoutMs = Number(args.timeout_ms ?? REQUEST_TIMEOUT_MS);
const startupTimeoutMs = Number(args.startup_timeout_ms ?? timeoutMs);
const sampleIntervalMs = Number(args.sample_interval_ms ?? DEFAULT_SAMPLE_INTERVAL_MS);
const preload = args.preload === 'true';
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
if (oldRepetitions !== newRepetitions) usage('counterbalanced benchmark requires equal --old-repetitions and --new-repetitions');
const balanced = await runCounterbalanced({ old: args.old, new: args.new }, request, oldRepetitions, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs);
const oldResult = balanced.old;
const newResult = balanced.new;
let snapshotStable = true;
let snapshotRevalidationError = null;
try {
  const finalSnapshotFiles = [];
  for (const file of snapshotFiles) finalSnapshotFiles.push(await fileDigest(file.path, canonicalIdentities));
  snapshotStable = JSON.stringify(finalSnapshotFiles.map((file) => [file.realpath, file.bytes, file.sha256, file.device, file.inode]))
    === JSON.stringify(snapshotFiles.map((file) => [file.realpath, file.bytes, file.sha256, file.device, file.inode]));
  if (!snapshotStable) snapshotRevalidationError = { code: 'SNAPSHOT_CHANGED', message: 'snapshot identity or digest changed during benchmark' };
} catch (error) {
  snapshotStable = false;
  snapshotRevalidationError = { code: 'SNAPSHOT_REVALIDATION_FAILED', message: error instanceof Error ? error.message : String(error) };
}
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
  snapshotStable,
  rpc,
  repetitions: { old: oldRepetitions, new: newRepetitions },
  executionOrder: balanced.executionOrder,
  orderDefinition: 'counterbalanced by repetition: old,new then new,old; both binaries use the same copied snapshot and sampler settings',
  timeoutMs,
  startupTimeoutMs,
  sampleIntervalMs,
  preload,
  timingDefinition: {
    preload: 'optional fresh native process ping; includes process startup and full snapshot load, elapsed from ping write until response line',
    cold: 'elapsed from vector_search request write until response line; with --preload this is the first retrieval after a completed preload',
    warm: 'same native process immediately after cold response; elapsed from vector_search request write until response line',
    p50: 'nearest-rank percentile over repetition wall-clock samples',
    p95: 'nearest-rank percentile over repetition wall-clock samples',
  },
  memoryDefinition: `${sampleIntervalMs}ms polling of /proc/$pid/status VmRSS/VmSwap and /proc/$pid/smaps_rollup Pss; peaks can under-sample short-lived maxima`,
  sampling: {
    intervalMs: sampleIntervalMs,
    sampleCount: {
      old: oldResult.sampleCount,
      new: newResult.sampleCount,
    },
    noSamplingControl: sampleIntervalMs === 0,
    overhead: 'sampling reads are included in the benchmark process and are deliberately disabled with --sample-interval-ms 0 for a counterfactual control; timings must be compared using the same setting',
  },
  binaries: { old: { ...oldResult, gitSha: args.old_sha ?? gitSha(args.old) }, new: { ...newResult, gitSha: args.new_sha ?? gitSha(args.new) } },
  parity,
  failures: [snapshotRevalidationError, ...[oldResult, newResult].flatMap((result, binaryIndex) => result.parity.flatMap((run, repetition) => {
    const failures = [];
    for (const phase of ['preload', 'cold', 'warm']) {
      if (run[phase] && run[phase].ok === false) failures.push({ binary: binaryIndex === 0 ? 'old' : 'new', repetition: repetition + 1, phase, error: run[phase].error });
    }
    return failures;
  }))].filter(Boolean),
};
await writeArtifactAtomically(resolve(args.out), `${JSON.stringify(artifact, null, 2)}\n`, protectedIdentities);
console.log(JSON.stringify({ out: resolve(args.out), snapshotHash, parity, old: artifact.binaries.old.p95Ms, new: artifact.binaries.new.p95Ms }));
if (artifact.failures.length > 0 || parity.some((entry) => !entry.coldEqual || !entry.warmEqual)) process.exitCode = 1;
