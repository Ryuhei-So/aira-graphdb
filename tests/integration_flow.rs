use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aira_graphdb::auth::AuthConfig;
use aira_graphdb::protocol::HandshakeRequest;
use aira_graphdb::query::execute_query;
use aira_graphdb::runtime::{DeploymentMode, RuntimeConfig, ServerRuntime};

// SystemTime is only microsecond-granular on macOS; pid plus a per-process
// sequence keeps names unique when tests run in parallel.
static NEXT_TEMP_DB: AtomicU64 = AtomicU64::new(0);

fn temp_db() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let seq = NEXT_TEMP_DB.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("agdb-int-{}-{nanos}-{seq}.db", std::process::id()))
}

#[test]
fn server_handshake_and_query_flow() {
    let db = temp_db();
    let runtime = ServerRuntime::start(
        RuntimeConfig {
            mode: DeploymentMode::Server,
            db_file: db.clone(),
            port: Some(7687),
            concurrency_profile: Some("P0-SERVER-CONCURRENCY".to_string()),
        },
        AuthConfig {
            allowed_algorithms: vec!["RS256".to_string()],
            expected_issuer: "https://issuer.example".to_string(),
            expected_audience: "aira-graphdb".to_string(),
            known_kids: vec!["k1".to_string()],
        },
    )
    .expect("server should start");

    runtime
        .handshake(HandshakeRequest {
            protocol_version: "protocol-p0@1.0.0".to_string(),
            canonical_type_system_version: "canonical-types@1.0.0".to_string(),
        })
        .expect("handshake should pass");

    let mut txm = runtime.tx_manager;
    let tx = txm.begin();
    {
        let store = txm.graph_mut(&tx).expect("tx graph");
        execute_query(store, "CREATE (n:Paper)").expect("create");
        execute_query(store, "MATCH (n) RETURN n").expect("match");
    }
    txm.commit(&tx).expect("commit");
    assert_eq!(txm.current_graph().node_count(), 1);

    let _ = std::fs::remove_file(&db);
    let _ = std::fs::remove_file(db.with_extension("wal"));
    let _ = std::fs::remove_file(db.with_extension("write.lock"));
}
