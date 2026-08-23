#!/usr/bin/env node

import { createHash } from 'node:crypto';
import { chmod, lstat, mkdtemp, open, readFile, readdir, realpath, rm } from 'node:fs/promises';
import { constants, existsSync, readFileSync } from 'node:fs';
import { spawn, spawnSync } from 'node:child_process';
import { basename, dirname, join, resolve } from 'node:path';
import { homedir, tmpdir } from 'node:os';

const DEFAULT_DIMENSIONS = 1024;
const DEFAULT_REPETITIONS = 4;
const REQUEST_TIMEOUT_MS = 300_000;
const DEFAULT_SAMPLE_INTERVAL_MS = 250;
const MAX_DIMENSIONS = 4096;
const MAX_TOP_K = 10_000;
const MAX_TIMEOUT_MS = 3_600_000;
const MAX_SAMPLE_INTERVAL_MS = 60_000;
const MAX_REPETITIONS = 32;
const DEFAULT_OVERALL_TIMEOUT_MS = 2 * 60 * 60 * 1000;
const MAX_OVERALL_TIMEOUT_MS = 4 * 60 * 60 * 1000;
const activeChildren = new Set();
let terminationSignal = null;

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, () => {
    terminationSignal = signal;
    for (const child of activeChildren) killGroup(child);
  });
}

function boundedInteger(value, name, { min, max }) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) usage(`${name} must be a safe integer in [${min}, ${max}]`);
  return parsed;
}

function usage(message) {
  const details = [
    'usage: native-vector-search-benchmark.mjs',
    '  --db SOURCE_JSON --blob SOURCE_VBLOB --old OLD_BINARY --new NEW_BINARY',
    '  --old-sha SHA --new-sha SHA',
    '  artifact is emitted on stdout; capture via a runner/API, never shell-redirect into live data paths',
    '  [--repetitions N] [--timeout-ms N] [--startup-timeout-ms N] [--sample-interval-ms N]',
    '  [--corpus ID] [--namespace NAME] [--top-k N]',
  ].join('\n');
  const error = new Error(message ? `${message}\n${details}` : details);
  error.code = 'USAGE';
  throw error;
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
  for (const required of ['db', 'blob', 'old', 'new', 'old_sha', 'new_sha']) {
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

async function openPrivateSource(path, label, { singleLink = true } = {}) {
  const absolute = resolve(path);
  const metadata = await lstat(absolute);
  if (metadata.isSymbolicLink() || !metadata.isFile() || (singleLink && metadata.nlink !== 1)) {
    throw new Error(`${label} must be a private regular inode: ${absolute}`);
  }
  const resolvedPath = await realpath(absolute);
  const handle = await open(absolute, constants.O_RDONLY | constants.O_NOFOLLOW);
  const held = await handle.stat();
  if (held.dev !== metadata.dev || held.ino !== metadata.ino || (singleLink && held.nlink !== 1) || !held.isFile()) {
    await handle.close();
    throw new Error(`${label} inode changed during validation: ${absolute}`);
  }
  return { path: absolute, realpath: resolvedPath, handle, device: held.dev, inode: held.ino, bytes: held.size, mtimeNs: held.mtimeNs };
}

async function copyPrivate(source, target, mode, deadline) {
  const output = await open(target, 'wx', mode);
  const digest = createHash('sha256');
  try {
    for await (const chunk of source.handle.createReadStream({ autoClose: false })) {
      if (terminationSignal) throw Object.assign(new Error(`terminated by ${terminationSignal}`), { code: 'TERMINATED' });
      if (Date.now() >= deadline) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
      digest.update(chunk);
      await output.write(chunk);
    }
    const after = await source.handle.stat();
    if (after.dev !== source.device || after.ino !== source.inode || after.size !== source.bytes || after.mtimeNs !== source.mtimeNs) {
      throw new Error(`source changed while copying: ${source.path}`);
    }
    await output.sync();
  } finally {
    await output.close();
    await source.handle.close();
  }
  await chmod(target, mode);
  const copied = await lstat(target);
  return { path: target, realpath: await realpath(target), bytes: copied.size, sha256: digest.digest('hex'), device: copied.dev, inode: copied.ino };
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

async function runBinary(binary, request, repetitions, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs, deadline) {
  const samples = [];
  let sampleCount = 0;
  const preloadMs = [];
  const coldMs = [];
  const warmMs = [];
  const parity = [];
  for (let repetition = 0; repetition < repetitions; repetition += 1) {
    if (terminationSignal) throw Object.assign(new Error(`terminated by ${terminationSignal}`), { code: 'TERMINATED' });
    const child = spawn(binary, ['--db', request.db], {
      detached: true,
      stdio: ['pipe', 'pipe', 'ignore'],
    });
    activeChildren.add(child);
    const initialMemory = sampleIntervalMs > 0 ? readMemory(child.pid) : null;
    if (initialMemory) { samples.push(initialMemory); sampleCount += 1; }
    const poll = sampleIntervalMs > 0 ? setInterval(() => {
      const memory = readMemory(child.pid);
      if (memory) { samples.push(memory); sampleCount += 1; }
    }, sampleIntervalMs) : null;
    try {
      if (preload) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
        const preloadStart = process.hrtime.bigint();
        try {
          assertRpcOk(await runRequest(child, { id: repetition * 3 + 1, method: 'ping', params: {} }, Math.min(startupTimeoutMs, remaining)), 'ping');
          preloadMs.push(Number(process.hrtime.bigint() - preloadStart) / 1e6);
        } catch (error) {
          const detail = errorDetail(error);
          preloadMs.push(Number(process.hrtime.bigint() - preloadStart) / 1e6);
          parity.push({ preload: { ok: false, error: detail }, cold: null, warm: null });
          continue;
        }
      }
      const coldStart = process.hrtime.bigint();
      const coldRemaining = deadline - Date.now();
      if (coldRemaining <= 0) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
      let cold;
      try {
        cold = assertRpcOk(await runRequest(child, { ...request.rpc, id: repetition * 3 + 2 }, Math.min(timeoutMs, coldRemaining)), 'vector_search');
        coldMs.push(Number(process.hrtime.bigint() - coldStart) / 1e6);
      } catch (error) {
        const detail = errorDetail(error);
        coldMs.push(Number(process.hrtime.bigint() - coldStart) / 1e6);
        parity.push({ cold: { ok: false, error: detail }, warm: null });
        continue;
      }
      const coldParity = resultParity(cold);
      const warmStart = process.hrtime.bigint();
      const warmRemaining = deadline - Date.now();
      if (warmRemaining <= 0) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
      try {
        const warm = assertRpcOk(await runRequest(child, { ...request.rpc, id: repetition * 3 + 3 }, Math.min(timeoutMs, warmRemaining)), 'vector_search');
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
      activeChildren.delete(child);
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

function publicResult(result) {
  const { binary, ...publicFields } = result;
  return publicFields;
}

async function runCounterbalanced(binaries, request, repetitions, timeoutMs, preload, startupTimeoutMs, sampleIntervalMs, deadline) {
  const runs = { old: [], new: [] };
  const executionOrder = [];
  for (let repetition = 0; repetition < repetitions; repetition += 1) {
    const sequence = repetition % 2 === 0 ? ['old', 'new'] : ['new', 'old'];
    for (const label of sequence) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
      executionOrder.push({ repetition: repetition + 1, binary: label });
      const result = await runBinary(binaries[label], request, 1, Math.min(timeoutMs, remaining), preload, Math.min(startupTimeoutMs, remaining), sampleIntervalMs, deadline);
      runs[label].push(result);
      const failed = result.parity.find((entry) => entry.preload?.ok === false || entry.cold?.ok === false || entry.warm?.ok === false);
      if (failed) throw Object.assign(new Error(failed.preload?.error?.message ?? failed.cold?.error?.message ?? failed.warm?.error?.message ?? 'native RPC failed'), { code: 'NATIVE_RPC_FAILED', nativeError: failed.preload?.error ?? failed.cold?.error ?? failed.warm?.error });
    }
  }
  return { old: combineRuns(runs.old), new: combineRuns(runs.new), executionOrder };
}

function gitSha(path) {
  const result = spawnSync('git', ['-C', dirname(path), 'rev-parse', 'HEAD'], { encoding: 'utf8' });
  return result.status === 0 ? result.stdout.trim() : null;
}

async function readBuildManifest(binary, suppliedSha, binaryHash) {
  const manifestPath = `${binary}.manifest.json`;
  const source = await openPrivateSource(manifestPath, 'binary build manifest');
  const manifest = JSON.parse(await source.handle.readFile('utf8'));
  await source.handle.close();
  const required = ['schema', 'sourceSha', 'binarySha256', 'cargoProfile', 'rustcVersion', 'buildCommand'];
  if (manifest.schema !== 'aira.native-build-manifest.v1' || required.some((key) => typeof manifest[key] !== 'string' || manifest[key].length === 0)) {
    usage(`invalid build manifest for ${basename(binary)}`);
  }
  if (manifest.sourceSha !== suppliedSha) usage(`build manifest source SHA mismatch for ${basename(binary)}`);
  if (manifest.binarySha256 !== binaryHash) usage(`build manifest binary hash mismatch for ${basename(binary)}`);
  return manifest;
}

async function main() {
  let workspace;
  try {
    const args = parseArgs(process.argv.slice(2));
    const overallTimeoutMs = boundedInteger(args.overall_timeout_ms ?? DEFAULT_OVERALL_TIMEOUT_MS, 'overall-timeout-ms', { min: 1, max: MAX_OVERALL_TIMEOUT_MS });
    const overallDeadline = Date.now() + overallTimeoutMs;
    const checkDeadline = () => {
      if (terminationSignal) throw Object.assign(new Error(`terminated by ${terminationSignal}`), { code: 'TERMINATED' });
      if (Date.now() >= overallDeadline) throw Object.assign(new Error('overall benchmark deadline exceeded'), { code: 'OVERALL_DEADLINE' });
    };
    checkDeadline();
    const sourceDb = resolve(args.db);
    const sourceBlob = resolve(args.blob);
    const canonical = process.env.LITERATURE_HUB_CANONICAL_DB && resolve(process.env.LITERATURE_HUB_CANONICAL_DB);
    if (canonical && (sourceDb === canonical || sourceBlob === canonical)) usage('refusing configured canonical DB/blob');
    if (sourceDb.includes('/literature-hub/semantic/data/') || sourceBlob.includes('/literature-hub/semantic/data/')) usage('refusing Literature Hub live-data path; pass a copied snapshot');
    if (!existsSync(sourceDb) || !existsSync(sourceBlob)) usage('source DB and --blob must exist');
    const dbSource = await openPrivateSource(sourceDb, 'source DB'); checkDeadline();
    const blobSource = await openPrivateSource(sourceBlob, 'source blob'); checkDeadline();
    const walPath = `${sourceDb.endsWith('.json') ? sourceDb.slice(0, -'.json'.length) : sourceDb}.agdb.wal`;
    if (existsSync(walPath)) usage('refusing source with WAL/recovery pending');
    const recoveryEntries = (await readdir(dirname(sourceDb))).filter((entry) => entry.includes('.recovery-') || entry.endsWith('.quarantine'));
    if (recoveryEntries.length > 0) usage(`refusing source recovery artifacts: ${recoveryEntries.join(', ')}`);
    const oldSource = await openPrivateSource(args.old, 'old binary', { singleLink: false }); checkDeadline();
    const newSource = await openPrivateSource(args.new, 'new binary', { singleLink: false }); checkDeadline();
    if (gitSha(args.old) !== args.old_sha) usage(`old binary SHA mismatch: supplied ${args.old_sha}`);
    if (gitSha(args.new) !== args.new_sha) usage(`new binary SHA mismatch: supplied ${args.new_sha}`);
    workspace = await mkdtemp(join(tmpdir(), 'aira-vector-benchmark-'));
    await chmod(workspace, 0o700);
    const db = join(workspace, basename(sourceDb));
    const blob = join(workspace, basename(sourceBlob));
    const old = join(workspace, `old-${basename(args.old)}`);
    const newer = join(workspace, `new-${basename(args.new)}`);
    const snapshotFiles = [await copyPrivate(dbSource, db, 0o600, overallDeadline), await copyPrivate(blobSource, blob, 0o600, overallDeadline)];
    const binaryFiles = { old: await copyPrivate(oldSource, old, 0o700, overallDeadline), new: await copyPrivate(newSource, newer, 0o700, overallDeadline) };
    if ((oldSource.device === newSource.device && oldSource.inode === newSource.inode) || binaryFiles.old.sha256 === binaryFiles.new.sha256 || args.old_sha === args.new_sha) {
      usage('old and new binaries must differ by inode, binary hash, and source SHA');
    }
    const oldManifest = await readBuildManifest(args.old, args.old_sha, binaryFiles.old.sha256); checkDeadline();
    const newManifest = await readBuildManifest(args.new, args.new_sha, binaryFiles.new.sha256); checkDeadline();
    const state = JSON.parse(await readFile(db, 'utf8'));
    const descriptorBlob = state.vectorBlob?.basename ? resolve(dirname(db), state.vectorBlob.basename) : null;
    if (descriptorBlob && dirname(descriptorBlob) !== dirname(db)) usage('vector blob descriptor must remain in private directory');
    if (!descriptorBlob) usage('atomic-generation benchmark requires a vectorBlob descriptor; legacy snapshots are rejected');
    if (descriptorBlob !== blob) usage(`--blob does not match generation descriptor: ${basename(descriptorBlob)}`);
    if (state.vectorBlob.size !== snapshotFiles[1].bytes || state.vectorBlob.sha256 !== snapshotFiles[1].sha256) {
      usage('private vectorBlob descriptor size/sha256 does not match copied blob');
    }
    if (!Number.isSafeInteger(state.generation) || state.generation < 1) usage('generation must be a safe positive integer');
    if (state.vectorBlob.format !== 1) usage('unsupported vector blob format');
    const snapshotHash = sha256Bytes(snapshotFiles.map((file) => `${basename(file.path)}:${file.sha256}\n`).join(''));

const dimensions = boundedInteger(args.dimensions ?? DEFAULT_DIMENSIONS, 'dimensions', { min: 1, max: MAX_DIMENSIONS });
const repetitions = boundedInteger(args.repetitions ?? DEFAULT_REPETITIONS, 'repetitions', { min: 2, max: MAX_REPETITIONS });
if (repetitions % 2 !== 0) usage('--repetitions must be even');
const timeoutMs = boundedInteger(args.timeout_ms ?? REQUEST_TIMEOUT_MS, 'timeout-ms', { min: 1, max: MAX_TIMEOUT_MS });
const startupTimeoutMs = boundedInteger(args.startup_timeout_ms ?? timeoutMs, 'startup-timeout-ms', { min: 1, max: MAX_TIMEOUT_MS });
const sampleIntervalMs = boundedInteger(args.sample_interval_ms ?? DEFAULT_SAMPLE_INTERVAL_MS, 'sample-interval-ms', { min: 0, max: MAX_SAMPLE_INTERVAL_MS });
const topK = boundedInteger(args.top_k ?? 10, 'top-k', { min: 1, max: MAX_TOP_K });
const queryVector = Array.from({ length: dimensions }, (_, index) => (index === 0 ? 1 : 0.001));
const rpc = {
  method: 'vector_search',
  params: {
    corpusId: args.corpus ?? 'libfull',
    namespace: args.namespace ?? 'fact',
    queryVector,
    topK,
  },
};
const request = { db, rpc };
const unsampled = await runCounterbalanced({ old, new: newer }, request, repetitions, timeoutMs, true, startupTimeoutMs, 0, overallDeadline);
const sampled = sampleIntervalMs === 0 ? unsampled : await runCounterbalanced({ old, new: newer }, request, repetitions, timeoutMs, true, startupTimeoutMs, sampleIntervalMs, overallDeadline);
const oldResult = unsampled.old;
const newResult = unsampled.new;
let snapshotStable = true;
let snapshotRevalidationError = null;
try {
  const finalSnapshotFiles = [];
  for (const file of snapshotFiles) finalSnapshotFiles.push(await fileDigest(file.path, new Set()));
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
const unsampledParity = unsampled.old.parity.map((oldRun, index) => ({
  repetition: index + 1,
  coldEqual: JSON.stringify(oldRun.cold) === JSON.stringify(unsampled.new.parity[index]?.cold),
  warmEqual: JSON.stringify(oldRun.warm) === JSON.stringify(unsampled.new.parity[index]?.warm),
}));

const artifact = {
  schema: 'aira.native-vector-search-benchmark.v2',
  generatedAt: new Date().toISOString(),
  copiedSnapshotOnly: true,
  snapshot: {
    source: {
      db: { basename: basename(dbSource.path), bytes: snapshotFiles[0].bytes, sha256: snapshotFiles[0].sha256 },
      blob: { basename: basename(blobSource.path), bytes: snapshotFiles[1].bytes, sha256: snapshotFiles[1].sha256 },
    },
    copied: { basenames: snapshotFiles.map((file) => basename(file.path)), snapshotHash, files: snapshotFiles.map((file) => ({ bytes: file.bytes, sha256: file.sha256 })) },
  },
  snapshotStable,
  generation: state.generation,
  vectorBlob: { format: state.vectorBlob.format, sha256: state.vectorBlob.sha256, size: state.vectorBlob.size },
  rpc,
  repetitions: { old: repetitions, new: repetitions },
  executionOrder: unsampled.executionOrder,
  sampledExecutionOrder: sampled.executionOrder,
  orderDefinition: 'both phases are counterbalanced by repetition: old,new then new,old; unsampled is the primary latency phase and sampled is the memory phase',
  timeoutMs,
  startupTimeoutMs,
  overallTimeoutMs,
  sampleIntervalMs,
  sampledPhaseExecuted: sampleIntervalMs !== 0,
  preload: true,
  timingDefinition: {
    preload: 'fresh native process ping; includes process startup and full snapshot load, elapsed from ping write until response line',
    cold: 'primary unsampled process-preloaded first retrieval; not a page-cache-cold claim',
    warm: 'same native process immediately after cold response; elapsed from vector_search request write until response line',
    p50: 'nearest-rank percentile over repetition wall-clock samples',
    p95: 'nearest-rank percentile over repetition wall-clock samples',
  },
  memoryDefinition: `${sampleIntervalMs}ms polling of /proc/$pid/status VmRSS/VmSwap and /proc/$pid/smaps_rollup Pss; sample-interval 0 performs zero /proc reads; peaks can under-sample short-lived maxima`,
  sampling: {
    intervalMs: sampleIntervalMs,
    sampleCount: {
      old: sampled.old.sampleCount,
      new: sampled.new.sampleCount,
    },
    noSamplingControl: true,
    overhead: 'primary latency is measured in the unsampled phase; sampled memory results are a separate counterbalanced phase and must not be compared for cross-phase timing or page-cache order',
  },
  binaries: {
    source: {
      old: { basename: basename(args.old), bytes: binaryFiles.old.bytes, gitSha: args.old_sha, sha256: binaryFiles.old.sha256 },
      new: { basename: basename(args.new), bytes: binaryFiles.new.bytes, gitSha: args.new_sha, sha256: binaryFiles.new.sha256 },
    },
    build: {
      old: { schema: oldManifest.schema, sourceSha: oldManifest.sourceSha, binarySha256: oldManifest.binarySha256, cargoProfile: oldManifest.cargoProfile, rustcVersion: oldManifest.rustcVersion, buildCommand: oldManifest.buildCommand },
      new: { schema: newManifest.schema, sourceSha: newManifest.sourceSha, binarySha256: newManifest.binarySha256, cargoProfile: newManifest.cargoProfile, rustcVersion: newManifest.rustcVersion, buildCommand: newManifest.buildCommand },
    },
    old: { ...publicResult(oldResult), gitSha: args.old_sha },
    new: { ...publicResult(newResult), gitSha: args.new_sha },
  },
  sampled: { old: publicResult(sampled.old), new: publicResult(sampled.new) },
  parity,
  unsampledParity,
  failures: [snapshotRevalidationError, ...[oldResult, newResult, sampled.old, sampled.new].flatMap((result, binaryIndex) => result.parity.flatMap((run, repetition) => {
    const failures = [];
    for (const phase of ['preload', 'cold', 'warm']) {
      if (run[phase] && run[phase].ok === false) failures.push({ binary: binaryIndex % 2 === 0 ? 'old' : 'new', repetition: repetition + 1, phase, error: run[phase].error });
    }
    return failures;
  }))].filter(Boolean),
};
console.log(JSON.stringify(artifact));
if (artifact.failures.length > 0 || parity.some((entry) => !entry.coldEqual || !entry.warmEqual) || unsampledParity.some((entry) => !entry.coldEqual || !entry.warmEqual)) process.exitCode = 1;
  } finally {
    if (workspace) await rm(workspace, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error.stack ?? error.message ?? String(error));
  const code = error.nativeError?.code ?? error.code ?? 'BENCHMARK_FAILED';
  const message = code === 'OVERALL_DEADLINE' ? 'overall benchmark deadline exceeded' : code === 'TERMINATED' ? 'benchmark terminated by signal' : code === 'NATIVE_RPC_FAILED' ? 'native RPC failed; see local stderr diagnostics' : 'benchmark failed; see local stderr diagnostics';
  console.log(JSON.stringify({ schema: 'aira.native-vector-search-benchmark.v2', failures: [{ code, message }] }));
  process.exitCode = terminationSignal ? 128 + (terminationSignal === 'SIGINT' ? 2 : 15) : 1;
});
