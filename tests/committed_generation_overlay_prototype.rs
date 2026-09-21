#[path = "support/committed_generation_overlay.rs"]
mod overlay;

use overlay::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

struct CountingAllocator;

static CURRENT_BYTES: AtomicIsize = AtomicIsize::new(0);
static PEAK_BYTES: AtomicIsize = AtomicIsize::new(0);
static REQUESTED_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);
static FAILED_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

fn add_bytes(bytes: isize) {
    let current = CURRENT_BYTES.fetch_add(bytes, Ordering::SeqCst) + bytes;
    let mut peak = PEAK_BYTES.load(Ordering::SeqCst);
    while current > peak {
        match PEAK_BYTES.compare_exchange(peak, current, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(observed) => peak = observed,
        }
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add_bytes(layout.size() as isize);
            REQUESTED_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
            ALLOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
        } else {
            FAILED_ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            add_bytes(layout.size() as isize);
            REQUESTED_BYTES.fetch_add(layout.size(), Ordering::SeqCst);
            ALLOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
        } else {
            FAILED_ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        add_bytes(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, old, new_size) };
        if !resized.is_null() {
            add_bytes(new_size as isize - old.size() as isize);
            REQUESTED_BYTES.fetch_add(new_size, Ordering::SeqCst);
            ALLOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
        } else {
            FAILED_ALLOCATIONS.fetch_add(1, Ordering::SeqCst);
        }
        resized
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn retained_bytes() -> isize {
    CURRENT_BYTES.load(Ordering::SeqCst)
}

fn reset_peak() {
    PEAK_BYTES.store(retained_bytes(), Ordering::SeqCst);
    REQUESTED_BYTES.store(0, Ordering::SeqCst);
    ALLOCATION_COUNT.store(0, Ordering::SeqCst);
    FAILED_ALLOCATIONS.store(0, Ordering::SeqCst);
}

fn resident_kib() -> usize {
    fs::read_to_string("/proc/self/status")
        .expect("Linux /proc/self/status is available")
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .and_then(|rest| rest.split_whitespace().next())
                .and_then(|value| value.parse().ok())
        })
        .expect("VmRSS is present")
}

#[test]
fn invariants() {
    let base = representative_base(7);
    let original_digest = base.stable_digest();
    let mut engine = PrototypeEngine::new(base, OverlayLimits::default());
    let base_identity = engine.base_identity();
    let lease = engine.committed_lease(7).expect("exact generation N");

    let delta = representative_delta("doc-c");
    engine
        .memory_upsert(&delta)
        .expect("bounded document delta");
    assert_eq!(
        engine.base_identity(),
        base_identity,
        "upsert replaced base Arc"
    );
    assert_eq!(engine.committed_generation(), 7);
    assert_eq!(lease.generation(), 7);
    assert_eq!(lease.stable_digest(), original_digest);
    assert!(lease.vector("corpus-1", "vector-c").is_none());
    assert!(lease.passage("corpus-1", "passage-c").is_none());
    assert_eq!(
        lease
            .fact("corpus-1", "fact-shared")
            .expect("committed shared fact")
            .source_document_ids,
        vec!["doc-a", "doc-b"]
    );
    assert_eq!(
        engine
            .fact("corpus-1", "fact-shared")
            .expect("writer sees updated fact")
            .source_document_ids,
        vec!["doc-a", "doc-b", "doc-c"]
    );
    assert!(engine.vector("corpus-1", "vector-c").is_some());
    assert!(engine.passage("corpus-1", "passage-c").is_some());
    assert!(engine.fact("corpus-1", "fact-c").is_some());
    assert_eq!(
        engine
            .schema("corpus-1", "schema-shared")
            .expect("writer sees updated schema")
            .fact_ids,
        vec!["fact-shared", "fact-a", "fact-c"]
    );

    let once = engine.overlay_digest();
    engine.memory_upsert(&delta).expect("idempotent replay");
    assert_eq!(engine.overlay_digest(), once);

    engine.discard();
    assert_eq!(engine.overlay_entry_count(), 0);
    assert!(engine.vector("corpus-1", "vector-c").is_none());
    assert_eq!(lease.stable_digest(), original_digest);

    engine
        .delete_document("corpus-1", "doc-a")
        .expect("bounded delete overlay");
    assert!(engine.vector("corpus-1", "vector-a").is_none());
    assert!(engine.passage("corpus-1", "passage-a").is_none());
    assert!(engine.fact("corpus-1", "fact-a").is_none());
    let shared_fact = engine
        .fact("corpus-1", "fact-shared")
        .expect("shared fact is preserved");
    assert_eq!(shared_fact.source_document_ids, vec!["doc-b"]);
    assert_eq!(shared_fact.passage_ids, vec!["passage-b"]);
    let shared_schema = engine
        .schema("corpus-1", "schema-shared")
        .expect("schema is retained");
    assert_eq!(shared_schema.source_document_ids, vec!["doc-b"]);
    assert_eq!(shared_schema.fact_ids, vec!["fact-shared"]);
    assert_eq!(shared_schema.frequency, 1);
    assert_eq!(shared_schema.state, "pending");
    assert!(engine.vector("corpus-1", "vector-b").is_some());
    assert!(lease.vector("corpus-1", "vector-a").is_some());
    assert_eq!(lease.stable_digest(), original_digest);
    let delete_digest = engine.overlay_digest();
    assert_eq!(
        engine.memory_upsert(&representative_delta("doc-a")),
        Err(PrototypeError::MixedDeleteWithPendingChanges)
    );
    assert_eq!(engine.overlay_digest(), delete_digest);

    assert_eq!(engine.publish(), Err(PrototypeError::ReadersActive));
    assert_eq!(engine.committed_generation(), 7);
    drop(lease);
    assert_eq!(engine.publish(), Ok(8));
    assert_eq!(engine.base_identity(), base_identity);
    assert_eq!(engine.overlay_entry_count(), 0);
    assert_eq!(engine.publish(), Err(PrototypeError::NoPendingChanges));
    assert!(matches!(
        engine.committed_lease(7),
        Err(PrototypeError::GenerationMismatch {
            requested: 7,
            committed: 8
        })
    ));
    let generation_8 = engine.committed_lease(8).expect("new exact generation");
    assert!(generation_8.vector("corpus-1", "vector-a").is_none());
    assert_eq!(
        generation_8
            .fact("corpus-1", "fact-shared")
            .expect("published shared fact")
            .source_document_ids,
        vec!["doc-b"]
    );

    let mut limited = PrototypeEngine::new(
        representative_base(11),
        OverlayLimits {
            max_entries: 2,
            ..OverlayLimits::default()
        },
    );
    let before_rejection = limited.overlay_digest();
    assert_eq!(
        limited.memory_upsert(&representative_delta("doc-c")),
        Err(PrototypeError::LimitExceeded("overlay entries"))
    );
    assert_eq!(limited.overlay_digest(), before_rejection);
    assert_eq!(limited.committed_generation(), 11);

    let mut invalid_id = representative_delta("doc-c");
    invalid_id.vectors[0].id = "i".repeat(4_097);
    let mut byte_limited = PrototypeEngine::new(
        representative_base(12),
        OverlayLimits {
            max_vector_bytes: 16,
            ..OverlayLimits::default()
        },
    );
    let unchanged = byte_limited.overlay_digest();
    assert_eq!(
        byte_limited.memory_upsert(&invalid_id),
        Err(PrototypeError::LimitExceeded("identifier bytes"))
    );
    assert_eq!(byte_limited.overlay_digest(), unchanged);
    assert_eq!(
        byte_limited.memory_upsert(&representative_delta("doc-c")),
        Err(PrototypeError::LimitExceeded("vector bytes"))
    );
    assert_eq!(byte_limited.overlay_digest(), unchanged);

    let mut non_finite = representative_delta("doc-c");
    non_finite.vectors[0].values[0] = f64::NAN;
    let mut rejecting = PrototypeEngine::new(representative_base(13), OverlayLimits::default());
    let unchanged = rejecting.overlay_digest();
    assert_eq!(
        rejecting.memory_upsert(&non_finite),
        Err(PrototypeError::InvalidDelta("non-finite vector value"))
    );
    assert_eq!(rejecting.overlay_digest(), unchanged);
    assert_eq!(
        rejecting.delete_document("corpus-1", ""),
        Err(PrototypeError::LimitExceeded("identifier bytes"))
    );
    assert_eq!(rejecting.overlay_digest(), unchanged);
    assert_eq!(
        rejecting.memory_upsert(&DocumentDelta {
            corpus_id: "corpus-1".into(),
            document_id: "doc-c".into(),
            ..DocumentDelta::default()
        }),
        Err(PrototypeError::InvalidDelta("empty document delta"))
    );
    assert_eq!(rejecting.overlay_digest(), unchanged);

    let mut exhausted =
        PrototypeEngine::new(representative_base(u64::MAX), OverlayLimits::default());
    exhausted
        .memory_upsert(&representative_delta("doc-c"))
        .expect("bounded delta before generation exhaustion");
    let exhausted_overlay = exhausted.overlay_digest();
    let exhausted_base = exhausted.base_identity();
    assert_eq!(
        exhausted.publish(),
        Err(PrototypeError::GenerationExhausted)
    );
    assert_eq!(exhausted.committed_generation(), u64::MAX);
    assert_eq!(exhausted.base_identity(), exhausted_base);
    assert_eq!(exhausted.overlay_digest(), exhausted_overlay);
}

#[test]
fn footprint() {
    let base_items = std::env::var("OVERLAY_BASE_ITEMS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000usize);
    assert!((1..=200_000).contains(&base_items));

    let base = synthetic_base(23, base_items);
    let delta = scaled_fixed_delta();
    let mut engine = PrototypeEngine::new(base, OverlayLimits::default());
    let base_identity = engine.base_identity();
    let rss_before_kib = resident_kib();
    let retained_before = retained_bytes();
    reset_peak();

    engine.memory_upsert(&delta).expect("fixed bounded delta");

    let retained_after = retained_bytes();
    let peak_after = PEAK_BYTES.load(Ordering::SeqCst);
    let actual_retained_delta = retained_after - retained_before;
    let actual_peak_delta = peak_after - retained_before;
    let requested_bytes = REQUESTED_BYTES.load(Ordering::SeqCst);
    let allocation_count = ALLOCATION_COUNT.load(Ordering::SeqCst);
    let failed_allocations = FAILED_ALLOCATIONS.load(Ordering::SeqCst);
    let semantic_overlay_bytes = engine.overlay_retained_bytes();
    let rss_after_kib = resident_kib();

    assert_eq!(engine.base_identity(), base_identity);
    assert_eq!(engine.overlay_entry_count(), 5);
    assert!(actual_retained_delta >= 0);
    assert!(actual_retained_delta < 512 * 1024);
    assert!(actual_peak_delta < 1024 * 1024);
    assert!(requested_bytes < 2 * 1024 * 1024);
    assert_eq!(failed_allocations, 0);
    assert!(semantic_overlay_bytes < 512 * 1024);

    let mut deleting_engine =
        PrototypeEngine::new(representative_base(29), OverlayLimits::default());
    let delete_retained_before = retained_bytes();
    reset_peak();
    deleting_engine
        .delete_document("corpus-1", "doc-a")
        .expect("bounded delete overlay");
    let delete_retained_delta = retained_bytes() - delete_retained_before;
    let delete_peak_delta = PEAK_BYTES.load(Ordering::SeqCst) - delete_retained_before;
    let delete_requested_bytes = REQUESTED_BYTES.load(Ordering::SeqCst);
    let delete_semantic_bytes = deleting_engine.overlay_retained_bytes();
    assert!(delete_retained_delta >= 0);
    assert!(delete_retained_delta < 512 * 1024);
    assert!(delete_peak_delta < 1024 * 1024);
    assert!(delete_requested_bytes < 2 * 1024 * 1024);
    assert!(delete_semantic_bytes < 512 * 1024);

    let mut oversized = representative_delta("doc-rejected");
    oversized.passages[0].text = "x".repeat(2 * 1024 * 1024);
    let mut rejecting_engine = PrototypeEngine::new(
        representative_base(31),
        OverlayLimits {
            max_retained_bytes: 64 * 1024,
            ..OverlayLimits::default()
        },
    );
    let rejected_digest = rejecting_engine.overlay_digest();
    let rejected_retained_before = retained_bytes();
    reset_peak();
    assert_eq!(
        rejecting_engine.memory_upsert(&oversized),
        Err(PrototypeError::LimitExceeded("text bytes"))
    );
    let rejected_retained_delta = retained_bytes() - rejected_retained_before;
    let rejected_peak_delta = PEAK_BYTES.load(Ordering::SeqCst) - rejected_retained_before;
    let rejected_requested_bytes = REQUESTED_BYTES.load(Ordering::SeqCst);
    assert_eq!(rejecting_engine.overlay_digest(), rejected_digest);
    assert_eq!(rejected_retained_delta, 0);
    assert!(rejected_peak_delta < 1024 * 1024);
    assert!(rejected_requested_bytes < 2 * 1024 * 1024);
    println!(
        "OVERLAY_METRIC {{\"base_items_per_collection\":{base_items},\"base_collections\":2,\"fixed_delta_records\":5,\"allocator_retained_before_bytes\":{retained_before},\"allocator_retained_delta_bytes\":{actual_retained_delta},\"allocator_peak_delta_bytes\":{actual_peak_delta},\"allocator_requested_bytes\":{requested_bytes},\"allocation_count\":{allocation_count},\"failed_allocations\":{failed_allocations},\"semantic_overlay_bytes\":{semantic_overlay_bytes},\"rss_before_kib\":{rss_before_kib},\"rss_after_kib\":{rss_after_kib},\"delete_retained_delta_bytes\":{delete_retained_delta},\"delete_peak_delta_bytes\":{delete_peak_delta},\"delete_requested_bytes\":{delete_requested_bytes},\"delete_semantic_bytes\":{delete_semantic_bytes},\"rejected_retained_delta_bytes\":{rejected_retained_delta},\"rejected_peak_delta_bytes\":{rejected_peak_delta},\"rejected_requested_bytes\":{rejected_requested_bytes}}}"
    );
}
