use super::*;
use aira_graphdb::unicode16_lowercase;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

const FAILURE_CLIENT: &str = "CLIENT_INPUT";
const FAILURE_INTEGRITY: &str = "INTEGRITY";
const FAILURE_DEADLINE: &str = "DEADLINE";
const FAILURE_RESOURCE: &str = "RESOURCE_LIMIT";
const REASON_VALIDATION: &str = "bounded request validation failed";
const REASON_INTEGRITY: &str = "bounded read integrity validation failed";
const REASON_DEADLINE: &str = "bounded operation deadline exceeded";
const REASON_RESOURCE: &str = "bounded operation resource limit exceeded";

#[derive(Debug)]
struct ExecutionFailure {
    class: &'static str,
    reason: &'static str,
    work: bounded_retrieval::WorkCounts,
}

impl ExecutionFailure {
    fn client(work: bounded_retrieval::WorkCounts) -> Self {
        Self {
            class: FAILURE_CLIENT,
            reason: REASON_VALIDATION,
            work,
        }
    }

    fn integrity(work: bounded_retrieval::WorkCounts) -> Self {
        Self {
            class: FAILURE_INTEGRITY,
            reason: REASON_INTEGRITY,
            work,
        }
    }

    fn deadline(work: bounded_retrieval::WorkCounts) -> Self {
        Self {
            class: FAILURE_DEADLINE,
            reason: REASON_DEADLINE,
            work,
        }
    }

    fn resource(work: bounded_retrieval::WorkCounts) -> Self {
        Self {
            class: FAILURE_RESOURCE,
            reason: REASON_RESOURCE,
            work,
        }
    }
}

struct ExecutionDeadline {
    start: Instant,
    deadline: bounded_retrieval::MonotonicDeadline,
    completed_units: u64,
    last_checkpoint_units: u64,
}

#[derive(Clone, Copy)]
struct SessionDeadlineCandidate {
    owner_remaining_ms: u64,
    absolute_deadline: Instant,
    native_remaining_ms: u64,
}

impl ExecutionDeadline {
    fn new(remaining_ms: u64) -> Result<Self, ExecutionFailure> {
        let deadline = bounded_retrieval::MonotonicDeadline::new(0, remaining_ms)
            .map_err(|_| ExecutionFailure::deadline(Default::default()))?;
        Ok(Self {
            start: Instant::now(),
            deadline,
            completed_units: 0,
            last_checkpoint_units: 0,
        })
    }

    fn now_ms(&self) -> u64 {
        self.start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn advance(
        &mut self,
        units: u64,
        work: bounded_retrieval::WorkCounts,
    ) -> Result<(), ExecutionFailure> {
        self.completed_units = self
            .completed_units
            .checked_add(units)
            .ok_or_else(|| ExecutionFailure::client(work))?;
        if self.completed_units - self.last_checkpoint_units
            >= bounded_retrieval::DEADLINE_CHECK_INTERVAL_UNITS
        {
            self.checkpoint(work)?;
        }
        Ok(())
    }

    fn checkpoint(&mut self, work: bounded_retrieval::WorkCounts) -> Result<(), ExecutionFailure> {
        let now = self.now_ms();
        self.deadline
            .checkpoint(now, self.completed_units)
            .map_err(|_| ExecutionFailure::deadline(work))?;
        self.last_checkpoint_units = self.completed_units;
        Ok(())
    }

    fn before_allocation(
        &mut self,
        work: bounded_retrieval::WorkCounts,
    ) -> Result<(), ExecutionFailure> {
        let now = self.now_ms();
        self.deadline
            .before_allocation(now, self.completed_units)
            .map_err(|_| ExecutionFailure::deadline(work))?;
        self.last_checkpoint_units = self.completed_units;
        Ok(())
    }

    fn before_materialization(
        &mut self,
        work: bounded_retrieval::WorkCounts,
    ) -> Result<(), ExecutionFailure> {
        let now = self.now_ms();
        self.before_materialization_at(now, work)
    }

    fn before_materialization_at(
        &mut self,
        now_ms: u64,
        work: bounded_retrieval::WorkCounts,
    ) -> Result<(), ExecutionFailure> {
        self.deadline
            .before_materialization(now_ms, self.completed_units)
            .map_err(|_| ExecutionFailure::deadline(work))?;
        self.last_checkpoint_units = self.completed_units;
        Ok(())
    }
}

fn utf16_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn ranked_cmp(left_score: f64, left_id: &str, right_score: f64, right_id: &str) -> Ordering {
    right_score
        .total_cmp(&left_score)
        .then_with(|| utf16_cmp(left_id, right_id))
}

fn bounded_cosine_with_query_norm(query: &[f64], query_norm: f64, vector: &[f64]) -> f64 {
    Server::cosine_with_query_norm(query, query_norm, vector).clamp(-1.0, 1.0)
}

fn bounded_query_norm(values: &[f64]) -> Option<f64> {
    if values.iter().any(|value| !value.is_finite()) {
        return None;
    }
    let scale = values.iter().map(|value| value.abs()).fold(0.0, f64::max);
    if scale == 0.0 {
        return Some(0.0);
    }
    Some(
        values
            .iter()
            .map(|value| {
                let scaled = value / scale;
                scaled * scaled
            })
            .sum::<f64>()
            .sqrt()
            * scale,
    )
}

struct RankedItem<T> {
    id: String,
    score: f64,
    payload: T,
}

impl<T> PartialEq for RankedItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.score.total_cmp(&other.score) == Ordering::Equal && self.id == other.id
    }
}

impl<T> Eq for RankedItem<T> {}

impl<T> PartialOrd for RankedItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for RankedItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // `ranked_cmp` orders best first, so BinaryHeap's greatest value is
        // the worst retained entry and can be evicted in O(log limit).
        ranked_cmp(self.score, &self.id, other.score, &other.id)
    }
}

fn bounded_rank_heap<T>(
    limit: usize,
    work: bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<BinaryHeap<RankedItem<T>>, ExecutionFailure> {
    deadline.before_allocation(work)?;
    let mut heap = BinaryHeap::new();
    heap.try_reserve(limit)
        .map_err(|_| ExecutionFailure::resource(work))?;
    deadline.before_allocation(work)?;
    Ok(heap)
}

fn retain_bounded_ranked<T>(
    retained: &mut BinaryHeap<RankedItem<T>>,
    id: &str,
    score: f64,
    payload: T,
    limit: usize,
) {
    if limit == 0 {
        return;
    }
    if retained.len() == limit {
        let worst = retained.peek().expect("non-empty bounded ranking");
        if ranked_cmp(score, id, worst.score, &worst.id) != Ordering::Less {
            return;
        }
    }
    retained.push(RankedItem {
        id: id.to_string(),
        score,
        payload,
    });
    if retained.len() > limit {
        retained.pop();
    }
}

fn finish_bounded_ranking<T>(retained: BinaryHeap<RankedItem<T>>) -> Vec<RankedItem<T>> {
    let mut ranked = retained.into_vec();
    ranked.sort_by(|left, right| ranked_cmp(left.score, &left.id, right.score, &right.id));
    ranked
}

fn account_graph_scan_unit(
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<(), ExecutionFailure> {
    work.graph_scan_units = work
        .graph_scan_units
        .checked_add(1)
        .ok_or_else(|| ExecutionFailure::resource(*work))?;
    deadline.advance(1, *work)
}

fn build_counted_graph_vec<T>(
    len: usize,
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
    mut build: impl FnMut(usize) -> T,
) -> Result<Vec<T>, ExecutionFailure> {
    deadline.before_allocation(*work)?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| ExecutionFailure::resource(*work))?;
    deadline.before_allocation(*work)?;
    for index in 0..len {
        values.push(build(index));
        account_graph_scan_unit(work, deadline)?;
    }
    Ok(values)
}

/// Bottom-up merge sort keeps producer-required UTF-16 order while making
/// every comparison and move observable and interruptible. Standard-library
/// sort cannot return a deadline failure from its comparator.
fn canonical_counted_sort<T: Copy>(
    values: &mut Vec<T>,
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
    mut compare: impl FnMut(T, T) -> Ordering,
) -> Result<(), ExecutionFailure> {
    if values.len() < 2 {
        return Ok(());
    }
    let mut scratch = build_counted_graph_vec(values.len(), work, deadline, |index| values[index])?;
    let mut width = 1_usize;
    let mut source_is_values = true;
    while width < values.len() {
        let (source, destination): (&[T], &mut [T]) = if source_is_values {
            (values.as_slice(), scratch.as_mut_slice())
        } else {
            (scratch.as_slice(), values.as_mut_slice())
        };
        let mut start = 0_usize;
        while start < source.len() {
            let middle = start.saturating_add(width).min(source.len());
            let end = middle.saturating_add(width).min(source.len());
            let (mut left, mut right) = (start, middle);
            for output in start..end {
                // Charge one deterministic merge-decision slot even after a
                // run is exhausted. Otherwise input order changes counters
                // despite producing the same canonical output.
                account_graph_scan_unit(work, deadline)?;
                let take_left = if left < middle && right < end {
                    compare(source[left], source[right]) != Ordering::Greater
                } else {
                    left < middle
                };
                destination[output] = if take_left {
                    let value = source[left];
                    left += 1;
                    value
                } else {
                    let value = source[right];
                    right += 1;
                    value
                };
                account_graph_scan_unit(work, deadline)?;
            }
            start = end;
        }
        source_is_values = !source_is_values;
        width = width
            .checked_mul(2)
            .ok_or_else(|| ExecutionFailure::resource(*work))?;
    }
    if !source_is_values {
        for (destination, source) in values.iter_mut().zip(&scratch) {
            *destination = *source;
            account_graph_scan_unit(work, deadline)?;
        }
    }
    Ok(())
}

fn canonical_sort_ids(
    values: &mut Vec<&str>,
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<(), ExecutionFailure> {
    canonical_counted_sort(values, work, deadline, utf16_cmp)
}

fn canonical_sort_edges(
    values: &mut Vec<&GraphEdge>,
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<(), ExecutionFailure> {
    canonical_counted_sort(values, work, deadline, |left, right| {
        utf16_cmp(&left.source_node_id, &right.source_node_id)
            .then_with(|| utf16_cmp(&left.target_node_id, &right.target_node_id))
            .then_with(|| left.weight.total_cmp(&right.weight))
    })
}

fn required_str<'a>(value: &'a Value, name: &str) -> Result<&'a str, ExecutionFailure> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ExecutionFailure::client(Default::default()))
}

fn required_u64(value: &Value, name: &str) -> Result<u64, ExecutionFailure> {
    value
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| ExecutionFailure::client(Default::default()))
}

fn required_f64(value: &Value, name: &str) -> Result<f64, ExecutionFailure> {
    value
        .get(name)
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
        .ok_or_else(|| ExecutionFailure::client(Default::default()))
}

fn stored_str<'a>(
    value: &'a Value,
    name: &str,
    work: bounded_retrieval::WorkCounts,
) -> Result<&'a str, ExecutionFailure> {
    value
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| ExecutionFailure::integrity(work))
}

fn bounded_stored_domain_id<'a>(
    value: &'a str,
    work: bounded_retrieval::WorkCounts,
) -> Result<&'a str, ExecutionFailure> {
    if value.is_empty() || value.len() > MAX_INDEXING_DOMAIN_ID_BYTES {
        return Err(ExecutionFailure::integrity(work));
    }
    Ok(value)
}

fn stored_snapshot_section_or_empty<'a>(
    snapshot: &'a Value,
    section: &str,
) -> Result<&'a [Value], ExecutionFailure> {
    match snapshot.get(section) {
        None => Ok(&[]),
        Some(Value::Array(items)) => Ok(items),
        Some(_) => Err(ExecutionFailure::integrity(Default::default())),
    }
}

fn normalize_fact_entity(
    value: &str,
    work: bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<String, ExecutionFailure> {
    let maximum = usize::try_from(bounded_retrieval::MAX_OBJECT_BYTES)
        .map_err(|_| ExecutionFailure::resource(work))?;
    if value.len() > maximum {
        return Err(ExecutionFailure::resource(work));
    }
    deadline.before_allocation(work)?;
    let mut scalar_steps = 0_u64;
    let normalized = unicode16_lowercase::lowercase_bounded(value, maximum, || {
        scalar_steps = scalar_steps
            .checked_add(1)
            .ok_or_else(|| ExecutionFailure::resource(work))?;
        if scalar_steps % bounded_retrieval::DEADLINE_CHECK_INTERVAL_UNITS == 0 {
            deadline.before_materialization(work)?;
        }
        Ok(())
    });
    let normalized = match normalized {
        Ok(value) => value,
        Err(unicode16_lowercase::LowercaseError::Checkpoint(failure)) => return Err(failure),
        Err(unicode16_lowercase::LowercaseError::Resource) => {
            return Err(ExecutionFailure::resource(work));
        }
    };
    deadline.before_materialization(work)?;
    Ok(normalized)
}

fn try_clone_json_string(
    value: &str,
    work: bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<String, ExecutionFailure> {
    deadline.before_allocation(work)?;
    let mut cloned = String::new();
    cloned
        .try_reserve_exact(value.len())
        .map_err(|_| ExecutionFailure::resource(work))?;
    cloned.push_str(value);
    deadline.before_materialization(work)?;
    Ok(cloned)
}

fn try_clone_json_value(
    value: &Value,
    work: bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<Value, ExecutionFailure> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Bool(value) => Ok(Value::Bool(*value)),
        Value::Number(value) => Ok(Value::Number(value.clone())),
        Value::String(value) => Ok(Value::String(try_clone_json_string(value, work, deadline)?)),
        Value::Array(values) => {
            deadline.before_allocation(work)?;
            let mut cloned = Vec::new();
            cloned
                .try_reserve_exact(values.len())
                .map_err(|_| ExecutionFailure::resource(work))?;
            for value in values {
                cloned.push(try_clone_json_value(value, work, deadline)?);
            }
            deadline.before_materialization(work)?;
            Ok(Value::Array(cloned))
        }
        Value::Object(values) => {
            // serde_json's default BTreeMap has no reserve API. Every
            // allocating payload (keys, strings, arrays) is nevertheless
            // reserved fallibly before insertion, and the complete object is
            // already bounded/serialized by `encode_object` below.
            let mut cloned = serde_json::Map::new();
            for (key, value) in values {
                let key = try_clone_json_string(key, work, deadline)?;
                let value = try_clone_json_value(value, work, deadline)?;
                cloned.insert(key, value);
            }
            deadline.before_materialization(work)?;
            Ok(Value::Object(cloned))
        }
    }
}

fn materialize_domain_object(
    value: &Value,
    work: &mut bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
) -> Result<Value, ExecutionFailure> {
    work.objects_considered_for_encoding = work
        .objects_considered_for_encoding
        .checked_add(1)
        .ok_or_else(|| ExecutionFailure::resource(*work))?;
    work.check()
        .map_err(|_| ExecutionFailure::resource(*work))?;
    deadline.before_materialization(*work)?;
    bounded_retrieval::encode_object(value).map_err(|_| ExecutionFailure::resource(*work))?;
    try_clone_json_value(value, *work, deadline)
}

fn encode_success_before_deadline(
    request_id: u64,
    generation: u64,
    value: &Value,
    work: bounded_retrieval::WorkCounts,
    deadline: &mut ExecutionDeadline,
    mut elapsed_ms: impl FnMut() -> u64,
) -> Result<Vec<u8>, ExecutionFailure> {
    deadline.before_materialization_at(elapsed_ms(), work)?;
    let frame = bounded_retrieval::encode_success(request_id, generation, value, work)
        .map_err(|_| ExecutionFailure::resource(work))?;
    deadline.before_materialization_at(elapsed_ms(), work)?;
    Ok(frame)
}

impl Server {
    fn bounded_index_key(corpus_id: &str, section: &str, item_id: &str) -> [u8; 32] {
        let mut hash = Sha256::new();
        for component in [corpus_id, section, item_id] {
            hash.update((component.len() as u64).to_le_bytes());
            hash.update(component.as_bytes());
        }
        hash.finalize().into()
    }

    fn preview_bounded_session_deadline(
        &self,
        owner_remaining_ms: u64,
        now: Instant,
    ) -> Result<SessionDeadlineCandidate, ExecutionFailure> {
        if self
            .bounded_session_owner_remaining_ms
            .is_some_and(|previous| owner_remaining_ms > previous)
        {
            return Err(ExecutionFailure::client(Default::default()));
        }
        let native_remaining_ms =
            owner_remaining_ms.min(bounded_retrieval::MAX_OPERATION_DEADLINE_MS);
        let proposed = now
            .checked_add(Duration::from_millis(native_remaining_ms))
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        let deadline = match self.bounded_session_deadline {
            Some(existing) => existing.min(proposed),
            None => proposed,
        };
        let native_remaining_ms =
            u64::try_from(deadline.saturating_duration_since(now).as_millis())
                .map_err(|_| ExecutionFailure::client(Default::default()))?;
        Ok(SessionDeadlineCandidate {
            owner_remaining_ms,
            absolute_deadline: deadline,
            native_remaining_ms,
        })
    }

    fn commit_bounded_session_deadline(&mut self, candidate: SessionDeadlineCandidate) {
        self.bounded_session_owner_remaining_ms = Some(candidate.owner_remaining_ms);
        self.bounded_session_deadline = Some(candidate.absolute_deadline);
    }

    pub(super) fn initialize_bounded_catalog(&mut self) -> io::Result<()> {
        self.ensure_bounded_catalog().map_err(|failure| {
            io::Error::other(format!(
                "descriptor bounded catalog initialization failed: {}",
                failure.reason
            ))
        })
    }

    fn ensure_bounded_catalog(&mut self) -> Result<(), ExecutionFailure> {
        if self.bounded_catalog_ready {
            return Ok(());
        }
        self.bounded_snapshot_indices.clear();
        self.bounded_edge_counts_by_corpus.clear();
        self.bounded_endpoint_counts_by_corpus.clear();
        let snapshot_item_count =
            self.state
                .snapshots
                .values()
                .try_fold(0_usize, |total, snapshot| {
                    ["passages", "facts", "schemas"].into_iter().try_fold(
                        total,
                        |subtotal, section| {
                            let count = stored_snapshot_section_or_empty(snapshot, section)?.len();
                            subtotal
                                .checked_add(count)
                                .ok_or_else(|| ExecutionFailure::resource(Default::default()))
                        },
                    )
                })?;
        self.bounded_snapshot_indices
            .try_reserve(snapshot_item_count)
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        for (corpus_id, snapshot) in &self.state.snapshots {
            if !snapshot.as_object().is_some_and(|object| {
                object.get("corpusId").and_then(Value::as_str) == Some(corpus_id)
            }) {
                return Err(ExecutionFailure::integrity(Default::default()));
            }
            for (section, id_field) in [
                ("passages", "passageId"),
                ("facts", "factId"),
                ("schemas", "schemaId"),
            ] {
                let items = stored_snapshot_section_or_empty(snapshot, section)?;
                for (index, item) in items.iter().enumerate() {
                    let item_id = item
                        .get(id_field)
                        .and_then(Value::as_str)
                        .ok_or_else(|| ExecutionFailure::integrity(Default::default()))?;
                    let item_id = bounded_stored_domain_id(item_id, Default::default())?;
                    if item.get("corpusId").and_then(Value::as_str) != Some(corpus_id) {
                        return Err(ExecutionFailure::integrity(Default::default()));
                    }
                    let key = Self::bounded_index_key(corpus_id, section, item_id);
                    if self.bounded_snapshot_indices.insert(key, index).is_some() {
                        return Err(ExecutionFailure::integrity(Default::default()));
                    }
                }
            }
        }
        if self.state.edges.len() as u64 > bounded_retrieval::MAX_GRAPH_EDGES {
            return Err(ExecutionFailure::resource(Default::default()));
        }
        let endpoint_capacity = self
            .state
            .edges
            .len()
            .checked_mul(2)
            .ok_or_else(|| ExecutionFailure::resource(Default::default()))?;
        let mut endpoint_ids = HashSet::<[u8; 32]>::new();
        endpoint_ids
            .try_reserve(endpoint_capacity)
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        for edge in self.state.edges.values() {
            for node_id in [&edge.source_node_id, &edge.target_node_id] {
                let node_id = bounded_stored_domain_id(node_id, Default::default())?;
                let node = self
                    .state
                    .nodes
                    .get(&Self::node_key(&edge.corpus_id, node_id))
                    .ok_or_else(|| ExecutionFailure::integrity(Default::default()))?;
                if node.corpus_id != edge.corpus_id || node.node_id != node_id {
                    return Err(ExecutionFailure::integrity(Default::default()));
                }
                if endpoint_ids.insert(Self::bounded_index_key(&edge.corpus_id, "nodes", node_id)) {
                    let count = self
                        .bounded_endpoint_counts_by_corpus
                        .entry(edge.corpus_id.clone())
                        .or_default();
                    *count = count
                        .checked_add(1)
                        .ok_or_else(|| ExecutionFailure::resource(Default::default()))?;
                    if *count > bounded_retrieval::MAX_GRAPH_NODES {
                        return Err(ExecutionFailure::resource(Default::default()));
                    }
                }
            }
            let count = self
                .bounded_edge_counts_by_corpus
                .entry(edge.corpus_id.clone())
                .or_default();
            *count = count
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(Default::default()))?;
        }
        self.bounded_catalog_ready = true;
        Ok(())
    }

    fn bounded_snapshot_items(
        &self,
        corpus_id: &str,
        section: &str,
    ) -> Result<&[Value], ExecutionFailure> {
        let snapshot = self
            .state
            .snapshots
            .get(corpus_id)
            .ok_or_else(|| ExecutionFailure::integrity(Default::default()))?;
        stored_snapshot_section_or_empty(snapshot, section)
    }

    fn bounded_snapshot_item(
        &self,
        corpus_id: &str,
        section: &str,
        item_id: &str,
    ) -> Result<&Value, ExecutionFailure> {
        let key = Self::bounded_index_key(corpus_id, section, item_id);
        let index = *self
            .bounded_snapshot_indices
            .get(&key)
            .ok_or_else(|| ExecutionFailure::integrity(Default::default()))?;
        let item = self
            .bounded_snapshot_items(corpus_id, section)?
            .get(index)
            .ok_or_else(|| ExecutionFailure::integrity(Default::default()))?;
        let id_field = match section {
            "passages" => "passageId",
            "facts" => "factId",
            "schemas" => "schemaId",
            _ => return Err(ExecutionFailure::integrity(Default::default())),
        };
        if item.get("corpusId").and_then(Value::as_str) != Some(corpus_id)
            || item.get(id_field).and_then(Value::as_str) != Some(item_id)
        {
            return Err(ExecutionFailure::integrity(Default::default()));
        }
        Ok(item)
    }

    pub(super) fn handle_bounded_frame(&mut self, frame: &[u8]) -> Vec<u8> {
        let parsed = bounded_retrieval::parse_request_frame(frame);
        let request = match parsed {
            Ok(request) => request,
            Err(_) => {
                return bounded_retrieval::encode_error(
                    0,
                    self.state.generation,
                    Default::default(),
                    FAILURE_CLIENT,
                    REASON_VALIDATION,
                )
                .unwrap_or_else(|_| b"{\"id\":0,\"ok\":false}\n".to_vec());
            }
        };
        let request_id = request.id;
        let result = self.execute_bounded(&request);
        match result {
            Ok((value, work, mut deadline)) => {
                let encoding_start = deadline.start;
                let encoded = encode_success_before_deadline(
                    request_id,
                    self.state.generation,
                    &value,
                    work,
                    &mut deadline,
                    || {
                        encoding_start
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX)
                    },
                );
                match encoded {
                    Ok(frame) => Ok(frame),
                    Err(failure) => bounded_retrieval::encode_error(
                        request_id,
                        self.state.generation,
                        failure.work,
                        failure.class,
                        failure.reason,
                    ),
                }
            }
            Err(failure) => bounded_retrieval::encode_error(
                request_id,
                self.state.generation,
                failure.work,
                failure.class,
                failure.reason,
            ),
        }
        .unwrap_or_else(|_| b"{\"id\":0,\"ok\":false}\n".to_vec())
    }

    fn execute_bounded(
        &mut self,
        request: &bounded_retrieval::BoundedRequest,
    ) -> Result<(Value, bounded_retrieval::WorkCounts, ExecutionDeadline), ExecutionFailure> {
        if !matches!(self.access_mode, AccessMode::DescriptorReadOnly(_)) {
            return Err(ExecutionFailure::client(Default::default()));
        }
        let candidate =
            self.preview_bounded_session_deadline(request.remaining_budget_ms, Instant::now())?;
        let mut deadline = ExecutionDeadline::new(candidate.native_remaining_ms)?;
        deadline.before_allocation(Default::default())?;
        let _admission = bounded_retrieval::admit_request(
            request,
            self.state.generation,
            bounded_retrieval::ReaderState::Idle,
        )
        .map_err(|_| ExecutionFailure::client(Default::default()))?;
        deadline.before_materialization(Default::default())?;
        self.commit_bounded_session_deadline(candidate);
        let (result, work) = match request.method.as_str() {
            bounded_retrieval::CANDIDATE_SEARCH => {
                self.execute_candidate_search(&request.params, &mut deadline)
            }
            bounded_retrieval::FACT_EXPAND => {
                self.execute_fact_expand(&request.params, &mut deadline)
            }
            bounded_retrieval::PPR_MATERIALIZE => self.execute_ppr(&request.params, &mut deadline),
            _ => Err(ExecutionFailure::client(Default::default())),
        }?;
        deadline.before_materialization(work)?;
        let normalization_failure = std::cell::RefCell::new(None);
        let deadline_ref = std::cell::RefCell::new(&mut deadline);
        let bounded_normalizer = |dependency: &str, value: &str| {
            if dependency != unicode16_lowercase::V15_ENTITY_NORMALIZATION_DIGEST {
                return None;
            }
            match normalize_fact_entity(value, work, &mut **deadline_ref.borrow_mut()) {
                Ok(value) => Some(value),
                Err(failure) => {
                    *normalization_failure.borrow_mut() = Some(failure);
                    None
                }
            }
        };
        let semantic_result = bounded_retrieval::validate_semantic_exchange(
            &request.method,
            &request.params,
            &result,
            &bounded_normalizer,
        );
        drop(deadline_ref);
        if let Some(failure) = normalization_failure.into_inner() {
            return Err(failure);
        }
        semantic_result.map_err(|_| ExecutionFailure::integrity(work))?;
        deadline.before_materialization(work)?;
        Ok((result, work, deadline))
    }

    fn execute_candidate_search(
        &self,
        params: &Value,
        deadline: &mut ExecutionDeadline,
    ) -> Result<(Value, bounded_retrieval::WorkCounts), ExecutionFailure> {
        let corpus_id = required_str(params, "corpusId")?;
        let slots = params
            .get("slots")
            .and_then(Value::as_array)
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        let comparisons = (self.state.vectors.len() as u64)
            .checked_mul(slots.len() as u64)
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        let mut usage = bounded_retrieval::ResourceUsage::for_request(
            bounded_retrieval::CANDIDATE_SEARCH,
            params,
        )
        .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        usage.vector_comparisons = comparisons;
        usage
            .check()
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        let result_capacity = slots.iter().try_fold(0_u64, |total, slot| {
            total
                .checked_add(required_u64(slot, "limit")?)
                .ok_or_else(|| ExecutionFailure::client(Default::default()))
        })?;
        bounded_retrieval::AllocationInput {
            vector_values: usage
                .vector_dimensions
                .checked_mul(usage.search_slots)
                .ok_or_else(|| ExecutionFailure::client(Default::default()))?,
            result_entries: result_capacity,
            heap_entries: result_capacity,
            retained_object_entries: result_capacity,
            response_buffer_bytes: bounded_retrieval::MAX_RESPONSE_FRAME_BYTES,
            ..Default::default()
        }
        .check()
        .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        deadline.before_allocation(Default::default())?;

        let mut work = bounded_retrieval::WorkCounts::default();
        let mut result_slots = Vec::with_capacity(slots.len());
        for slot in slots {
            let slot_id = required_str(slot, "slotId")?;
            let namespace = required_str(slot, "namespace")?;
            let threshold = required_f64(slot, "threshold")?;
            let limit = usize::try_from(required_u64(slot, "limit")?)
                .map_err(|_| ExecutionFailure::client(work))?;
            let query = slot
                .get("queryVector")
                .and_then(Value::as_array)
                .ok_or_else(|| ExecutionFailure::client(work))?
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .filter(|number| number.is_finite())
                        .ok_or_else(|| ExecutionFailure::client(work))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let query_norm =
                bounded_query_norm(&query).ok_or_else(|| ExecutionFailure::client(work))?;
            let mut retained = bounded_rank_heap(limit, work, deadline)?;
            for (key, record) in &self.state.vectors {
                work.vector_comparisons = work
                    .vector_comparisons
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::client(work))?;
                deadline.advance(1, work)?;
                if record.corpus_id != corpus_id || record.namespace != namespace {
                    continue;
                }
                let record_id = bounded_stored_domain_id(&record.id, work)?;
                let vector = self
                    .vector_values
                    .get(key)
                    .ok_or_else(|| ExecutionFailure::integrity(work))?;
                if vector.len() != query.len() || vector.iter().any(|value| !value.is_finite()) {
                    return Err(ExecutionFailure::integrity(work));
                }
                let score = bounded_cosine_with_query_norm(&query, query_norm, vector);
                if score >= threshold {
                    retain_bounded_ranked(&mut retained, record_id, score, (), limit);
                }
            }
            let (section, prefix) = match namespace {
                "passage" => ("passages", "passage:"),
                "fact" => ("facts", "fact:"),
                "schema" => ("schemas", "schema:"),
                _ => return Err(ExecutionFailure::client(work)),
            };
            let mut hits = Vec::with_capacity(retained.len());
            for RankedItem { id, score, .. } in finish_bounded_ranking(retained) {
                let item_id = id
                    .strip_prefix(prefix)
                    .ok_or_else(|| ExecutionFailure::integrity(work))?;
                let item = self.bounded_snapshot_item(corpus_id, section, item_id)?;
                let item = materialize_domain_object(item, &mut work, deadline)?;
                hits.push(json!({"id": id, "score": score, "item": item}));
            }
            result_slots.push(json!({"slotId": slot_id, "namespace": namespace, "hits": hits}));
        }
        work.check().map_err(|_| ExecutionFailure::resource(work))?;
        Ok((json!({"slots": result_slots}), work))
    }

    fn execute_fact_expand(
        &self,
        params: &Value,
        deadline: &mut ExecutionDeadline,
    ) -> Result<(Value, bounded_retrieval::WorkCounts), ExecutionFailure> {
        let corpus_id = required_str(params, "corpusId")?;
        let plan = params
            .get("plan")
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        let facts = self.bounded_snapshot_items(corpus_id, "facts")?;
        let mut usage =
            bounded_retrieval::ResourceUsage::for_request(bounded_retrieval::FACT_EXPAND, params)
                .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        usage.facts_inspected = facts.len() as u64;
        usage
            .check()
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        let limit = usize::try_from(required_u64(plan, "limit")?)
            .map_err(|_| ExecutionFailure::client(Default::default()))?;
        bounded_retrieval::AllocationInput {
            seed_entries: usage.seeds,
            result_entries: limit as u64,
            heap_entries: limit as u64,
            retained_object_entries: limit as u64,
            response_buffer_bytes: bounded_retrieval::MAX_RESPONSE_FRAME_BYTES,
            scratch_bytes: bounded_retrieval::MAX_OBJECT_BYTES * 6,
            ..Default::default()
        }
        .check()
        .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        deadline.before_allocation(Default::default())?;
        let attenuation = required_f64(plan, "attenuation")?;
        let seeds = plan
            .get("seedEntities")
            .and_then(Value::as_array)
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?
            .iter()
            .map(|seed| {
                Ok((
                    required_str(seed, "key")?.to_string(),
                    required_f64(seed, "score")?,
                ))
            })
            .collect::<Result<HashMap<_, _>, ExecutionFailure>>()?;
        let excluded = plan
            .get("excludedSeedFactIds")
            .and_then(Value::as_array)
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or_else(|| ExecutionFailure::client(Default::default()))
            })
            .collect::<Result<HashSet<_>, _>>()?;
        let mut work = bounded_retrieval::WorkCounts::default();
        let mut retained = bounded_rank_heap(limit, work, deadline)?;
        for fact in facts {
            work.facts_inspected = work
                .facts_inspected
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::client(work))?;
            deadline.advance(1, work)?;
            let fact_id = stored_str(fact, "factId", work)?;
            if excluded.contains(fact_id) {
                continue;
            }
            let head =
                normalize_fact_entity(stored_str(fact, "headEntity", work)?, work, deadline)?;
            let tail =
                normalize_fact_entity(stored_str(fact, "tailEntity", work)?, work, deadline)?;
            let head_score = seeds.get(&head).copied();
            let tail_score = seeds.get(&tail).copied();
            if head_score.is_none() && tail_score.is_none() {
                continue;
            }
            let score = head_score.unwrap_or(0.0).max(tail_score.unwrap_or(0.0)) * attenuation;
            if !score.is_finite() {
                return Err(ExecutionFailure::integrity(work));
            }
            retain_bounded_ranked(&mut retained, fact_id, score, fact, limit);
        }
        let facts = finish_bounded_ranking(retained)
            .into_iter()
            .map(
                |RankedItem {
                     id: fact_id,
                     score,
                     payload: fact,
                 }| {
                    let fact = materialize_domain_object(fact, &mut work, deadline)?;
                    Ok(json!({"factId": fact_id, "score": score, "fact": fact}))
                },
            )
            .collect::<Result<Vec<_>, ExecutionFailure>>()?;
        Ok((json!({"facts": facts}), work))
    }

    fn execute_ppr(
        &self,
        params: &Value,
        deadline: &mut ExecutionDeadline,
    ) -> Result<(Value, bounded_retrieval::WorkCounts), ExecutionFailure> {
        let corpus_id = required_str(params, "corpusId")?;
        let plan = params
            .get("plan")
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        let graph_nodes = self
            .bounded_endpoint_counts_by_corpus
            .get(corpus_id)
            .copied()
            .unwrap_or(0);
        let graph_edges = self
            .bounded_edge_counts_by_corpus
            .get(corpus_id)
            .copied()
            .unwrap_or(0);
        let mut usage = bounded_retrieval::ResourceUsage::for_request(
            bounded_retrieval::PPR_MATERIALIZE,
            params,
        )
        .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        usage.graph_nodes = graph_nodes;
        usage.graph_edges = graph_edges;
        usage
            .check()
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        let combined_limit = usage
            .returned_passages
            .checked_add(usage.returned_facts)
            .ok_or_else(|| ExecutionFailure::client(Default::default()))?;
        bounded_retrieval::AllocationInput {
            seed_entries: usage.seeds,
            result_entries: combined_limit,
            ppr_score_entries: graph_nodes,
            ppr_node_entries: graph_nodes,
            ppr_edge_entries: graph_edges,
            heap_entries: combined_limit,
            retained_object_entries: combined_limit,
            response_buffer_bytes: bounded_retrieval::MAX_RESPONSE_FRAME_BYTES,
            ..Default::default()
        }
        .check()
        .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        deadline.before_allocation(Default::default())?;
        if graph_nodes == 0 {
            return Ok((
                json!({
                    "rankedPassages": [], "rankedFacts": [], "iterations": 0,
                    "converged": true, "l1Delta": 0.0
                }),
                Default::default(),
            ));
        }

        // The admitted endpoint and edge counts bound this reconstruction.
        // Never scan the full node catalog: isolated nodes are outside the
        // PPR graph and must not create unreported O(corpus) work.
        let graph_node_capacity = usize::try_from(graph_nodes)
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        let mut endpoint_ids = HashSet::<&str>::new();
        deadline.before_allocation(Default::default())?;
        endpoint_ids
            .try_reserve(graph_node_capacity)
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        deadline.before_allocation(Default::default())?;
        let mut node_ids = Vec::<&str>::new();
        deadline.before_allocation(Default::default())?;
        node_ids
            .try_reserve_exact(graph_node_capacity)
            .map_err(|_| ExecutionFailure::resource(Default::default()))?;
        deadline.before_allocation(Default::default())?;
        let mut work = bounded_retrieval::WorkCounts::default();
        let edge_capacity =
            usize::try_from(graph_edges).map_err(|_| ExecutionFailure::resource(work))?;
        let mut edges = Vec::<&GraphEdge>::new();
        deadline.before_allocation(work)?;
        edges
            .try_reserve_exact(edge_capacity)
            .map_err(|_| ExecutionFailure::resource(work))?;
        deadline.before_allocation(work)?;
        for edge in self.state.edges.values() {
            account_graph_scan_unit(&mut work, deadline)?;
            if edge.corpus_id == corpus_id {
                edges.push(edge);
            }
        }
        if edges.len() as u64 != graph_edges {
            return Err(ExecutionFailure::integrity(work));
        }
        canonical_sort_edges(&mut edges, &mut work, deadline)?;
        for edge in &edges {
            if endpoint_ids.insert(edge.source_node_id.as_str()) {
                node_ids.push(edge.source_node_id.as_str());
                work.nodes_initialized = work
                    .nodes_initialized
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                work.graph_scan_units = work
                    .graph_scan_units
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                deadline.advance(1, work)?;
            }
            if endpoint_ids.insert(edge.target_node_id.as_str()) {
                node_ids.push(edge.target_node_id.as_str());
                work.nodes_initialized = work
                    .nodes_initialized
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                work.graph_scan_units = work
                    .graph_scan_units
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                deadline.advance(1, work)?;
            }
            work.edges_visited = work
                .edges_visited
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            work.graph_scan_units = work
                .graph_scan_units
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            deadline.advance(1, work)?;
        }
        canonical_sort_ids(&mut node_ids, &mut work, deadline)?;
        if node_ids.len() as u64 != graph_nodes {
            return Err(ExecutionFailure::integrity(work));
        }
        if work.nodes_initialized != graph_nodes || work.edges_visited != graph_edges {
            return Err(ExecutionFailure::integrity(work));
        }
        deadline.before_allocation(work)?;
        let mut index = HashMap::new();
        index
            .try_reserve(node_ids.len())
            .map_err(|_| ExecutionFailure::resource(work))?;
        deadline.before_allocation(work)?;
        for (node_index, node_id) in node_ids.iter().enumerate() {
            if index.insert(*node_id, node_index).is_some() {
                return Err(ExecutionFailure::integrity(work));
            }
            account_graph_scan_unit(&mut work, deadline)?;
        }
        let mut out_degree =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |_| 0_usize)?;
        let mut in_degree =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |_| 0_u64)?;
        let mut outgoing_weight_sums =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |_| 0.0_f64)?;
        for edge in &edges {
            if !edge.weight.is_finite() || edge.weight < 0.0 {
                return Err(ExecutionFailure::integrity(work));
            }
            let source = *index
                .get(edge.source_node_id.as_str())
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            let target = *index
                .get(edge.target_node_id.as_str())
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            out_degree[source] = out_degree[source]
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            outgoing_weight_sums[source] += edge.weight;
            if !outgoing_weight_sums[source].is_finite() {
                return Err(ExecutionFailure::integrity(work));
            }
            in_degree[target] = in_degree[target]
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            work.graph_scan_units = work
                .graph_scan_units
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            deadline.advance(1, work)?;
        }
        let mut running_offset = 0_usize;
        let mut offsets =
            build_counted_graph_vec(node_ids.len() + 1, &mut work, deadline, |node_index| {
                let offset = running_offset;
                if node_index < out_degree.len() {
                    running_offset = running_offset.saturating_add(out_degree[node_index]);
                }
                offset
            })?;
        if running_offset != edges.len() {
            return Err(ExecutionFailure::integrity(work));
        }
        // Freeze the sentinel explicitly; saturating construction above is
        // checked by the exact edge total before this point.
        offsets[node_ids.len()] = running_offset;
        let mut cursors =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |node_index| {
                offsets[node_index]
            })?;
        let mut adjacency =
            build_counted_graph_vec(edges.len(), &mut work, deadline, |_| (0_usize, 0.0_f64))?;
        for edge in &edges {
            let source = *index
                .get(edge.source_node_id.as_str())
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            let target = *index
                .get(edge.target_node_id.as_str())
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            let position = cursors[source];
            let slot = adjacency
                .get_mut(position)
                .ok_or_else(|| ExecutionFailure::integrity(work))?;
            *slot = (target, edge.weight);
            cursors[source] = position
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            account_graph_scan_unit(&mut work, deadline)?;
        }
        for (node_index, cursor) in cursors.iter().enumerate() {
            if *cursor != offsets[node_index + 1] {
                return Err(ExecutionFailure::integrity(work));
            }
            account_graph_scan_unit(&mut work, deadline)?;
        }
        let hub_threshold = required_u64(plan, "hubDegreeThreshold")?;
        let hub_damping =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |node_index| {
                let total = (out_degree[node_index] as u64).saturating_add(in_degree[node_index]);
                let node_id = node_ids[node_index];
                if node_id.starts_with("schema:") && total > hub_threshold {
                    1.0 / ((total + 2) as f64).log2()
                } else {
                    1.0
                }
            })?;
        let mut teleport =
            build_counted_graph_vec(node_ids.len(), &mut work, deadline, |_| 0.0_f64)?;
        let mut teleport_sum = 0.0;
        let mut seeds = plan
            .get("seeds")
            .and_then(Value::as_array)
            .ok_or_else(|| ExecutionFailure::client(work))?
            .iter()
            .map(|seed| Ok((required_str(seed, "nodeId")?, required_f64(seed, "score")?)))
            .collect::<Result<Vec<_>, ExecutionFailure>>()?;
        seeds.sort_by(|left, right| utf16_cmp(left.0, right.0));
        for (node_id, score) in seeds {
            if let Some(seed_index) = index.get(node_id) {
                teleport[*seed_index] = score;
                teleport_sum += score;
            }
        }
        if teleport_sum > 0.0 {
            for value in &mut teleport {
                *value /= teleport_sum;
                account_graph_scan_unit(&mut work, deadline)?;
            }
        } else {
            let uniform = 1.0 / node_ids.len() as f64;
            for value in &mut teleport {
                *value = uniform;
                account_graph_scan_unit(&mut work, deadline)?;
            }
        }

        let teleport_probability = required_f64(plan, "teleportProbability")?;
        let epsilon = required_f64(plan, "convergenceEpsilon")?;
        let max_iterations = required_u64(plan, "maxIterations")?;
        let mut scores =
            build_counted_graph_vec(teleport.len(), &mut work, deadline, |node_index| {
                teleport[node_index]
            })?;
        let mut converged = false;
        let mut iterations = 0_u64;
        let mut l1_delta = 0.0;
        for iteration in 0..max_iterations {
            let mut next =
                build_counted_graph_vec(node_ids.len(), &mut work, deadline, |_| 0.0_f64)?;
            for source in 0..node_ids.len() {
                work.graph_scan_units = work
                    .graph_scan_units
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                deadline.advance(1, work)?;
                let outgoing = &adjacency[offsets[source]..offsets[source + 1]];
                if outgoing.is_empty() {
                    continue;
                }
                let total_weight = outgoing_weight_sums[source];
                if !total_weight.is_finite() || total_weight <= 0.0 {
                    return Err(ExecutionFailure::integrity(work));
                }
                for (target, weight) in outgoing {
                    next[*target] += (1.0 - teleport_probability)
                        * scores[source]
                        * (*weight / total_weight)
                        * hub_damping[*target];
                    work.graph_scan_units = work
                        .graph_scan_units
                        .checked_add(1)
                        .ok_or_else(|| ExecutionFailure::resource(work))?;
                    deadline.advance(1, work)?;
                }
            }
            for (value, teleport_value) in next.iter_mut().zip(&teleport) {
                *value += teleport_probability * teleport_value;
                work.graph_scan_units = work
                    .graph_scan_units
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                deadline.advance(1, work)?;
            }
            l1_delta = 0.0;
            for (next_value, previous) in next.iter().zip(&scores) {
                if !next_value.is_finite() {
                    return Err(ExecutionFailure::integrity(work));
                }
                l1_delta += (next_value - previous).abs();
                work.graph_scan_units = work
                    .graph_scan_units
                    .checked_add(1)
                    .ok_or_else(|| ExecutionFailure::resource(work))?;
                deadline.advance(1, work)?;
            }
            if !l1_delta.is_finite() {
                return Err(ExecutionFailure::integrity(work));
            }
            scores = next;
            iterations = iteration + 1;
            work.iterations = iterations;
            if l1_delta < epsilon {
                converged = true;
                break;
            }
        }

        let passage_limit = usize::try_from(required_u64(plan, "passageLimit")?)
            .map_err(|_| ExecutionFailure::client(work))?;
        let fact_limit = usize::try_from(required_u64(plan, "entityLimit")?)
            .map_err(|_| ExecutionFailure::client(work))?;
        let mut passages = bounded_rank_heap(passage_limit, work, deadline)?;
        let mut facts = bounded_rank_heap(fact_limit, work, deadline)?;
        for (node_id, score) in node_ids.iter().zip(&scores) {
            if node_id.starts_with("passage:") {
                retain_bounded_ranked(&mut passages, node_id, *score, (), passage_limit);
            } else if node_id.starts_with("fact:") {
                retain_bounded_ranked(&mut facts, node_id, *score, (), fact_limit);
            }
            work.graph_scan_units = work
                .graph_scan_units
                .checked_add(1)
                .ok_or_else(|| ExecutionFailure::resource(work))?;
            deadline.advance(1, work)?;
        }
        let ranked_passages = finish_bounded_ranking(passages)
            .into_iter()
            .enumerate()
            .map(|(rank, RankedItem { id: node_id, score, .. })| {
                let passage_id = node_id
                    .strip_prefix("passage:")
                    .ok_or_else(|| ExecutionFailure::integrity(work))?;
                let passage = self.bounded_snapshot_item(corpus_id, "passages", passage_id)?;
                let passage = materialize_domain_object(passage, &mut work, deadline)?;
                Ok(json!({"nodeId": node_id, "score": score, "rank": rank + 1, "passage": passage}))
            })
            .collect::<Result<Vec<_>, ExecutionFailure>>()?;
        let ranked_facts = finish_bounded_ranking(facts)
            .into_iter()
            .enumerate()
            .map(
                |(
                    rank,
                    RankedItem {
                        id: node_id, score, ..
                    },
                )| {
                    let fact_id = node_id
                        .strip_prefix("fact:")
                        .ok_or_else(|| ExecutionFailure::integrity(work))?;
                    let fact = self.bounded_snapshot_item(corpus_id, "facts", fact_id)?;
                    let fact = materialize_domain_object(fact, &mut work, deadline)?;
                    Ok(json!({"nodeId": node_id, "score": score, "rank": rank + 1, "fact": fact}))
                },
            )
            .collect::<Result<Vec<_>, ExecutionFailure>>()?;
        work.check().map_err(|_| ExecutionFailure::resource(work))?;
        Ok((
            json!({
                "rankedPassages": ranked_passages,
                "rankedFacts": ranked_facts,
                "iterations": iterations,
                "converged": converged,
                "l1Delta": l1_delta,
            }),
            work,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn producer_fixture() -> Value {
        serde_json::from_slice(include_bytes!(
            "../../../spec/contracts/bounded-retrieval/bounded-retrieval-fixture.json"
        ))
        .expect("producer fixture parses")
    }

    fn test_server() -> Server {
        let fixture = producer_fixture();
        let candidate = &fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["result"];
        let mut passages = Vec::new();
        let mut facts = Vec::new();
        let mut schemas = Vec::new();
        let mut vectors = HashMap::new();
        let mut vector_values = HashMap::new();
        for slot in candidate["slots"].as_array().expect("candidate slots") {
            let namespace = slot["namespace"].as_str().expect("namespace");
            for hit in slot["hits"].as_array().expect("hits") {
                let id = hit["id"].as_str().expect("id").to_string();
                match namespace {
                    "passage" => passages.push(hit["item"].clone()),
                    "fact" => facts.push(hit["item"].clone()),
                    "schema" => schemas.push(hit["item"].clone()),
                    _ => unreachable!(),
                }
                let key = Server::key("fixture-corpus", &id);
                vectors.insert(
                    key.clone(),
                    VectorRecord {
                        id,
                        corpus_id: "fixture-corpus".to_string(),
                        namespace: namespace.to_string(),
                        values: Vec::new(),
                        blob_ref: None,
                        metadata: json!({}),
                    },
                );
                vector_values.insert(key, vec![1.0]);
            }
        }
        let edge_rows = [
            ("e1", "passage:p1", "fact:f1"),
            ("e2", "passage:p2", "fact:f2"),
            ("e3", "fact:f1", "passage:p2"),
            ("e4", "fact:f2", "passage:p1"),
        ];
        let nodes = edge_rows
            .iter()
            .flat_map(|(_, source, target)| [*source, *target])
            .collect::<HashSet<_>>()
            .into_iter()
            .map(|node_id| {
                (
                    Server::node_key("fixture-corpus", node_id),
                    GraphNode {
                        node_id: node_id.to_string(),
                        corpus_id: "fixture-corpus".to_string(),
                        layer: node_id.split_once(':').unwrap().0.to_string(),
                        r#ref: json!({}),
                        label: node_id.to_string(),
                    },
                )
            })
            .collect();
        let edges = edge_rows
            .into_iter()
            .map(|(edge_id, source, target)| {
                (
                    Server::key("fixture-corpus", edge_id),
                    GraphEdge {
                        edge_id: edge_id.to_string(),
                        corpus_id: "fixture-corpus".to_string(),
                        source_node_id: source.to_string(),
                        target_node_id: target.to_string(),
                        relation: "fixture".to_string(),
                        weight: 1.0,
                        bridge_kind: None,
                    },
                )
            })
            .collect();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            "fixture-corpus".to_string(),
            json!({
                "corpusId": "fixture-corpus",
                "schemaVersion": 1,
                "passages": passages,
                "facts": facts,
                "schemas": schemas,
            }),
        );
        let mut server = Server {
            db_path: None,
            audit_log_path: None,
            state: State {
                nodes,
                edges,
                vectors,
                snapshots,
                generation: 7,
                ..Default::default()
            },
            vector_values,
            vector_blob_lineage: Vec::new(),
            cache_dirty: true,
            transaction: TransactionState::Idle,
            wal_path: None,
            wal_bytes: 0,
            wal_record_count: 0,
            wal_hasher: Sha256::new(),
            wal_file: None,
            wal_identity: None,
            active_mutation_request_ids: HashSet::new(),
            wal_replaying: false,
            last_persist_bytes: 0,
            fatal: false,
            node_keys_by_corpus: HashMap::new(),
            edge_keys_by_corpus: HashMap::new(),
            adjacent_edge_keys_by_node: HashMap::new(),
            vector_keys_by_corpus_namespace: HashMap::new(),
            passage_keys_by_corpus: HashMap::new(),
            bounded_snapshot_indices: HashMap::new(),
            bounded_edge_counts_by_corpus: HashMap::new(),
            bounded_endpoint_counts_by_corpus: HashMap::new(),
            bounded_catalog_ready: false,
            bounded_session_deadline: None,
            bounded_session_owner_remaining_ms: None,
            access_mode: AccessMode::DescriptorReadOnly(DescriptorReadHandshake {
                canonical_sha256: "0".repeat(64),
                vector_blob_sha256: "0".repeat(64),
                vector_blob_size: 0,
                legacy_generation0: false,
                legacy_binding_sha256: None,
                method_inventory_sha256: "0".repeat(64),
            }),
        };
        server.initialize_bounded_catalog().expect("catalog");
        server
    }

    fn bounded_frame(id: u64, method: &str, params: Value, remaining_budget_ms: u64) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(&json!({
            "id": id,
            "method": method,
            "expectedGeneration": 7,
            "remainingBudgetMs": remaining_budget_ms,
            "params": params,
        }))
        .expect("request encodes");
        bytes.push(b'\n');
        bytes
    }

    fn response(bytes: Vec<u8>) -> Value {
        serde_json::from_slice(&bytes).expect("response parses")
    }

    #[test]
    fn all_three_bounded_operations_execute_without_snapshot_response() {
        let fixture = producer_fixture();
        let mut server = test_server();
        let state_before = serde_json::to_vec(&server.state).expect("state serializes");
        for (id, method) in bounded_retrieval::BOUNDED_OPERATIONS.iter().enumerate() {
            let params = fixture["exchanges"][*method]["request"].clone();
            let output = response(server.handle_bounded_frame(&bounded_frame(
                id as u64 + 1,
                method,
                params,
                60_000 - id as u64 * 1_000,
            )));
            assert_eq!(output["ok"], true, "{method}: {output}");
            assert_eq!(output["generation"], 7);
            assert!(output.get("result").is_some());
            assert!(output.get("counters").is_some());
            assert!(serde_json::to_vec(&output).unwrap().len() < 2 * 1024 * 1024);
        }
        assert_eq!(serde_json::to_vec(&server.state).unwrap(), state_before);
        assert!(server.wal_file.is_none());
        assert_eq!(server.wal_bytes, 0);
    }

    #[test]
    fn deadline_stale_generation_and_unknown_envelope_fail_without_result() {
        let fixture = producer_fixture();
        let params = fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone();
        let mut server = test_server();
        let expired = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::CANDIDATE_SEARCH,
            params.clone(),
            0,
        )));
        assert_eq!(expired["ok"], false);
        assert!(expired.get("result").is_none());
        assert_eq!(expired["error"]["failureClass"], FAILURE_DEADLINE);

        let mut stale = bounded_frame(
            2,
            bounded_retrieval::CANDIDATE_SEARCH,
            params.clone(),
            60_000,
        );
        let mut stale_value: Value = serde_json::from_slice(&stale).unwrap();
        stale_value["expectedGeneration"] = json!(6);
        stale = serde_json::to_vec(&stale_value).unwrap();
        stale.push(b'\n');
        let stale = response(server.handle_bounded_frame(&stale));
        assert_eq!(stale["ok"], false);
        assert!(stale.get("result").is_none());

        let mut unknown: Value = serde_json::from_slice(&bounded_frame(
            3,
            bounded_retrieval::CANDIDATE_SEARCH,
            params,
            60_000,
        ))
        .unwrap();
        unknown["future"] = json!(true);
        let mut unknown = serde_json::to_vec(&unknown).unwrap();
        unknown.push(b'\n');
        let unknown = response(server.handle_bounded_frame(&unknown));
        assert_eq!(unknown["ok"], false);
        assert!(unknown.get("result").is_none());
    }

    #[test]
    fn success_frame_is_withheld_when_deadline_expires_during_encoding() {
        let work = bounded_retrieval::WorkCounts::default();
        let mut deadline = ExecutionDeadline::new(100).unwrap();
        let mut elapsed = [0_u64, 101].into_iter();
        let failure = encode_success_before_deadline(
            1,
            7,
            &json!({"bounded": true}),
            work,
            &mut deadline,
            || elapsed.next().unwrap(),
        )
        .expect_err("late success frame must be discarded");
        assert_eq!(failure.class, FAILURE_DEADLINE);
        assert_eq!(failure.work.checked_work_units().unwrap(), 0);
    }

    #[test]
    fn ppr_generation_counts_reject_before_request_graph_reconstruction() {
        let fixture = producer_fixture();
        let params = fixture["exchanges"][bounded_retrieval::PPR_MATERIALIZE]["request"].clone();
        let mut server = test_server();
        server.bounded_endpoint_counts_by_corpus.insert(
            "fixture-corpus".to_string(),
            bounded_retrieval::MAX_GRAPH_NODES + 1,
        );
        // If admission incorrectly traverses the request graph first, this
        // deliberately broken cache would surface as INTEGRITY instead.
        server.node_keys_by_corpus.clear();
        let output = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::PPR_MATERIALIZE,
            params,
            60_000,
        )));
        assert_eq!(output["ok"], false);
        assert_eq!(output["error"]["failureClass"], FAILURE_RESOURCE);
        assert!(output.get("result").is_none());
        assert_eq!(output["counters"]["workUnits"], 0);
    }

    #[test]
    fn ppr_reconstruction_uses_admitted_edge_endpoints_not_the_full_node_catalog() {
        let fixture = producer_fixture();
        let params = fixture["exchanges"][bounded_retrieval::PPR_MATERIALIZE]["request"].clone();
        let mut server = test_server();
        server.node_keys_by_corpus.clear();
        let output = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::PPR_MATERIALIZE,
            params,
            60_000,
        )));
        assert_eq!(output["ok"], true, "{output}");
    }

    #[test]
    fn bounded_cosine_clamps_finite_rounding_to_the_contract_range() {
        let vector = [-0.35957717938031397, 0.2639059139809068];
        let norm = bounded_query_norm(&vector).expect("finite norm");
        assert_eq!(bounded_cosine_with_query_norm(&vector, norm, &vector), 1.0);
    }

    #[test]
    fn bounded_cosine_preserves_subnormal_finite_vectors() {
        let vector = [f64::from_bits(1)];
        let norm = bounded_query_norm(&vector).expect("finite subnormal norm");
        assert_eq!(norm, vector[0]);
        assert_eq!(bounded_cosine_with_query_norm(&vector, norm, &vector), 1.0);
    }

    #[test]
    fn bounded_rank_heap_keeps_only_canonical_top_k() {
        let work = bounded_retrieval::WorkCounts::default();
        let mut deadline = ExecutionDeadline::new(60_000).unwrap();
        let mut heap = bounded_rank_heap(3, work, &mut deadline).unwrap();
        for (id, score) in [("z", 1.0), ("😀", 3.0), ("a", 3.0), ("b", 2.0), ("x", -1.0)] {
            retain_bounded_ranked(&mut heap, id, score, (), 3);
        }
        let ranked = finish_bounded_ranking(heap);
        assert_eq!(
            ranked
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "😀", "b"]
        );
        assert_eq!(
            ranked.iter().map(|item| item.score).collect::<Vec<_>>(),
            vec![3.0, 3.0, 2.0]
        );
    }

    #[test]
    fn canonical_endpoint_sort_is_utf16_ordered_and_fully_counted() {
        let mut ids = vec!["😀", "aa", "é", "a"];
        let mut expected = ids.clone();
        expected.sort_by(|left, right| utf16_cmp(left, right));
        let mut work = bounded_retrieval::WorkCounts::default();
        let mut deadline = ExecutionDeadline::new(60_000).unwrap();
        canonical_sort_ids(&mut ids, &mut work, &mut deadline).unwrap();
        assert_eq!(ids, expected);
        assert!(work.graph_scan_units >= ids.len() as u64);
        assert_eq!(work.checked_work_units().unwrap(), work.graph_scan_units);
    }

    #[test]
    fn canonical_edge_sort_orders_every_floating_accumulation_input() {
        let edges = [
            GraphEdge {
                edge_id: "z".into(),
                corpus_id: "c".into(),
                source_node_id: "😀".into(),
                target_node_id: "b".into(),
                relation: "r".into(),
                weight: 2.0,
                bridge_kind: None,
            },
            GraphEdge {
                edge_id: "y".into(),
                corpus_id: "c".into(),
                source_node_id: "a".into(),
                target_node_id: "z".into(),
                relation: "r".into(),
                weight: 3.0,
                bridge_kind: None,
            },
            GraphEdge {
                edge_id: "x".into(),
                corpus_id: "c".into(),
                source_node_id: "a".into(),
                target_node_id: "z".into(),
                relation: "r".into(),
                weight: 1.0,
                bridge_kind: None,
            },
        ];
        let mut ordered = vec![&edges[0], &edges[1], &edges[2]];
        let mut work = bounded_retrieval::WorkCounts::default();
        let mut deadline = ExecutionDeadline::new(60_000).unwrap();
        canonical_sort_edges(&mut ordered, &mut work, &mut deadline).unwrap();
        assert_eq!(
            ordered
                .iter()
                .map(|edge| edge.edge_id.as_str())
                .collect::<Vec<_>>(),
            vec!["x", "y", "z"]
        );
        assert!(work.graph_scan_units > ordered.len() as u64);
    }

    #[test]
    fn bounded_identity_digest_is_length_framed_and_stable() {
        let key = Server::bounded_index_key("a", "bc", "d");
        assert_eq!(key, Server::bounded_index_key("a", "bc", "d"));
        assert_ne!(key, Server::bounded_index_key("ab", "c", "d"));
        assert_ne!(key, Server::bounded_index_key("a", "b", "cd"));
    }

    #[test]
    fn ppr_seed_accumulation_is_canonical_across_request_order() {
        let fixture = producer_fixture();
        let mut canonical =
            fixture["exchanges"][bounded_retrieval::PPR_MATERIALIZE]["request"].clone();
        canonical["plan"]["seeds"] = json!([
            {"nodeId": "fact:f1", "score": 1.0e16},
            {"nodeId": "fact:f2", "score": 1.0},
            {"nodeId": "passage:p1", "score": -1.0e16}
        ]);
        let mut permuted = canonical.clone();
        permuted["plan"]["seeds"] = json!([
            {"nodeId": "passage:p1", "score": -1.0e16},
            {"nodeId": "fact:f1", "score": 1.0e16},
            {"nodeId": "fact:f2", "score": 1.0}
        ]);

        let canonical_output = response(test_server().handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::PPR_MATERIALIZE,
            canonical,
            60_000,
        )));
        let permuted_output = response(test_server().handle_bounded_frame(&bounded_frame(
            2,
            bounded_retrieval::PPR_MATERIALIZE,
            permuted,
            60_000,
        )));
        assert_eq!(canonical_output["ok"], true, "{canonical_output}");
        assert_eq!(permuted_output["ok"], true, "{permuted_output}");
        assert_eq!(canonical_output["result"], permuted_output["result"]);
        assert_eq!(canonical_output["counters"], permuted_output["counters"]);
    }

    #[test]
    fn descriptor_session_remainder_cannot_increase_between_operations() {
        let mut server = test_server();
        let start = Instant::now();
        let candidate = server
            .preview_bounded_session_deadline(1_000, start)
            .unwrap();
        assert_eq!(candidate.native_remaining_ms, 1_000);
        server.commit_bounded_session_deadline(candidate);
        let candidate = server
            .preview_bounded_session_deadline(900, start + Duration::from_millis(100))
            .unwrap();
        assert_eq!(candidate.native_remaining_ms, 900);
        server.commit_bounded_session_deadline(candidate);
        assert_eq!(
            server
                .preview_bounded_session_deadline(900, start + Duration::from_millis(101))
                .unwrap()
                .native_remaining_ms,
            899
        );
        assert!(
            server
                .preview_bounded_session_deadline(901, start + Duration::from_millis(102))
                .is_err()
        );

        let mut capped = test_server();
        let candidate = capped
            .preview_bounded_session_deadline(65_000, start)
            .unwrap();
        assert_eq!(
            candidate.native_remaining_ms,
            bounded_retrieval::MAX_OPERATION_DEADLINE_MS
        );
        capped.commit_bounded_session_deadline(candidate);
        assert!(
            capped
                .preview_bounded_session_deadline(70_000, start + Duration::from_millis(1))
                .is_err()
        );

        let fixture = producer_fixture();
        let request = fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone();
        let mut through_wire = test_server();
        assert_eq!(
            response(through_wire.handle_bounded_frame(&bounded_frame(
                1,
                bounded_retrieval::CANDIDATE_SEARCH,
                request.clone(),
                65_000,
            )))["ok"],
            true
        );
        let increased = response(through_wire.handle_bounded_frame(&bounded_frame(
            2,
            bounded_retrieval::CANDIDATE_SEARCH,
            request.clone(),
            70_000,
        )));
        assert_eq!(increased["ok"], false);
        assert_eq!(increased["error"]["failureClass"], FAILURE_CLIENT);

        let mut invalid_does_not_poison = test_server();
        let mut invalid = request.clone();
        invalid["future"] = json!(true);
        let invalid = response(invalid_does_not_poison.handle_bounded_frame(&bounded_frame(
            3,
            bounded_retrieval::CANDIDATE_SEARCH,
            invalid,
            1,
        )));
        assert_eq!(invalid["error"]["failureClass"], FAILURE_CLIENT);
        assert!(invalid_does_not_poison.bounded_session_deadline.is_none());
        let valid = response(invalid_does_not_poison.handle_bounded_frame(&bounded_frame(
            4,
            bounded_retrieval::CANDIDATE_SEARCH,
            request,
            60_000,
        )));
        assert_eq!(valid["ok"], true, "{valid}");
    }

    #[test]
    fn aggregate_result_caps_reject_before_bounded_work() {
        let fixture = producer_fixture();
        let mut candidate =
            fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone();
        for slot in candidate["slots"].as_array_mut().unwrap() {
            slot["limit"] = json!(bounded_retrieval::MAX_SEARCH_RESULT_LIMIT);
        }
        let candidate = response(test_server().handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::CANDIDATE_SEARCH,
            candidate,
            60_000,
        )));
        assert_eq!(candidate["error"]["failureClass"], FAILURE_CLIENT);
        assert_eq!(candidate["counters"]["workUnits"], 0);

        let mut ppr = fixture["exchanges"][bounded_retrieval::PPR_MATERIALIZE]["request"].clone();
        ppr["plan"]["passageLimit"] = json!(bounded_retrieval::MAX_RETURNED_PASSAGES);
        ppr["plan"]["entityLimit"] = json!(bounded_retrieval::MAX_RETURNED_FACTS);
        let ppr = response(test_server().handle_bounded_frame(&bounded_frame(
            2,
            bounded_retrieval::PPR_MATERIALIZE,
            ppr,
            60_000,
        )));
        assert_eq!(ppr["error"]["failureClass"], FAILURE_CLIENT);
        assert_eq!(ppr["counters"]["workUnits"], 0);
    }

    #[test]
    fn ppr_integrity_failure_reports_only_completed_reconstruction_work() {
        let fixture = producer_fixture();
        let params = fixture["exchanges"][bounded_retrieval::PPR_MATERIALIZE]["request"].clone();
        let mut server = test_server();
        server
            .state
            .edges
            .get_mut("fixture-corpus:e1")
            .unwrap()
            .weight = f64::NAN;
        let output = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::PPR_MATERIALIZE,
            params,
            60_000,
        )));
        assert_eq!(output["error"]["failureClass"], FAILURE_INTEGRITY);
        assert_eq!(output["counters"]["nodesInitialized"], 4);
        assert_eq!(output["counters"]["edgesVisited"], 4);
        assert_eq!(output["counters"]["graphScanUnits"], 70);
        assert_eq!(output["counters"]["workUnits"], 70);
    }

    #[test]
    fn bounded_catalog_rejects_snapshot_key_and_corpus_identity_mismatch() {
        let mut server = test_server();
        server.state.snapshots.get_mut("fixture-corpus").unwrap()["corpusId"] =
            json!("wrong-corpus");
        server.bounded_catalog_ready = false;
        assert!(server.initialize_bounded_catalog().is_err());
    }

    #[test]
    fn bounded_catalog_preserves_absent_as_empty_snapshot_sections() {
        let mut server = test_server();
        server.state.snapshots.insert(
            "legacy-empty".to_string(),
            json!({"corpusId": "legacy-empty"}),
        );
        server.bounded_catalog_ready = false;
        server
            .initialize_bounded_catalog()
            .expect("missing legacy sections are empty");
        assert!(
            server
                .bounded_snapshot_items("legacy-empty", "facts")
                .unwrap()
                .is_empty()
        );

        server.state.snapshots.get_mut("legacy-empty").unwrap()["facts"] = json!({});
        server.bounded_catalog_ready = false;
        assert!(server.initialize_bounded_catalog().is_err());
    }

    #[test]
    fn stored_domain_ids_are_bounded_before_utf16_ranking() {
        let oversized = "x".repeat(MAX_INDEXING_DOMAIN_ID_BYTES + 1);

        let mut graph_server = test_server();
        graph_server
            .state
            .edges
            .values_mut()
            .next()
            .unwrap()
            .source_node_id = oversized.clone();
        graph_server.bounded_catalog_ready = false;
        assert!(graph_server.initialize_bounded_catalog().is_err());

        let mut snapshot_server = test_server();
        snapshot_server
            .state
            .snapshots
            .get_mut("fixture-corpus")
            .unwrap()["facts"][0]["factId"] = json!(oversized.clone());
        snapshot_server.bounded_catalog_ready = false;
        assert!(snapshot_server.initialize_bounded_catalog().is_err());

        let fixture = producer_fixture();
        let mut vector_server = test_server();
        for record in vector_server.state.vectors.values_mut() {
            record.id = oversized.clone();
        }
        let candidate = response(vector_server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::CANDIDATE_SEARCH,
            fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone(),
            60_000,
        )));
        assert_eq!(candidate["ok"], false);
        assert_eq!(candidate["error"]["failureClass"], FAILURE_INTEGRITY);
        assert!(candidate.get("result").is_none());
    }

    #[test]
    fn stored_vector_and_fact_corruption_are_integrity_failures() {
        let fixture = producer_fixture();
        let mut server = test_server();
        let vector_key = server.vector_values.keys().next().unwrap().clone();
        server.vector_values.insert(vector_key, vec![1.0, 2.0]);
        let candidate = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::CANDIDATE_SEARCH,
            fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone(),
            60_000,
        )));
        assert_eq!(candidate["ok"], false);
        assert_eq!(candidate["error"]["failureClass"], FAILURE_INTEGRITY);
        assert!(candidate.get("result").is_none());

        let facts = server.state.snapshots.get_mut("fixture-corpus").unwrap()["facts"]
            .as_array_mut()
            .unwrap();
        facts[0].as_object_mut().unwrap().remove("headEntity");
        let expanded = response(server.handle_bounded_frame(&bounded_frame(
            2,
            bounded_retrieval::FACT_EXPAND,
            fixture["exchanges"][bounded_retrieval::FACT_EXPAND]["request"].clone(),
            59_000,
        )));
        assert_eq!(expanded["ok"], false);
        assert_eq!(expanded["error"]["failureClass"], FAILURE_INTEGRITY);
        assert!(expanded.get("result").is_none());
    }

    #[test]
    fn oversized_selected_domain_object_is_resource_failure_without_result() {
        let fixture = producer_fixture();
        let mut server = test_server();
        let expected_work = server.state.vectors.len() as u64 + 1;
        let passages = server.state.snapshots.get_mut("fixture-corpus").unwrap()["passages"]
            .as_array_mut()
            .unwrap();
        for passage in passages {
            passage["text"] =
                Value::String("x".repeat(bounded_retrieval::MAX_OBJECT_BYTES as usize));
        }
        let output = response(server.handle_bounded_frame(&bounded_frame(
            1,
            bounded_retrieval::CANDIDATE_SEARCH,
            fixture["exchanges"][bounded_retrieval::CANDIDATE_SEARCH]["request"].clone(),
            60_000,
        )));
        assert_eq!(output["ok"], false);
        assert_eq!(output["error"]["failureClass"], FAILURE_RESOURCE);
        assert!(output.get("result").is_none());
        assert_eq!(output["counters"]["objectsConsideredForEncoding"], 1);
        assert_eq!(output["counters"]["workUnits"], expected_work);
    }
}
