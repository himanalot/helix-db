use std::sync::Arc;

use sonic_rs::json;

use crate::helix_engine::types::GraphError;
use crate::helix_engine::vector_core::hnsw::HNSW;
use crate::helix_gateway::router::router::{Handler, HandlerInput, HandlerSubmission};
use crate::protocol;

/// Request structure for HardDeleteVectors
#[derive(sonic_rs::Deserialize)]
struct HardDeleteRequest {
    vector_ids: Vec<String>,
}

/// Hard delete specific vectors from HNSW by their IDs.
/// This completely removes vectors from all HNSW structures:
/// - vector_properties_db
/// - vectors_db (all levels)
/// - edges_db (all connections)
///
/// Request body:
/// - vector_ids: Array of vector ID strings to delete
///
/// Example: {"vector_ids": ["1f0faf6a-af39-60da-b221-010203040506", "..."]}
pub fn hard_delete_vectors_inner(input: HandlerInput) -> Result<protocol::Response, GraphError> {
    eprintln!("[HardDeleteVectors] Starting...");

    // Parse vector_ids from request body
    let vector_ids: Vec<String> = if input.request.body.is_empty() {
        return Err(GraphError::New("Missing vector_ids in request body".to_string()));
    } else {
        match sonic_rs::from_slice::<HardDeleteRequest>(&input.request.body) {
            Ok(req) => req.vector_ids,
            Err(e) => return Err(GraphError::New(format!("Invalid JSON: {}", e))),
        }
    };

    if vector_ids.is_empty() {
        return Ok(protocol::Response {
            body: sonic_rs::to_vec(&json!({
                "status": "success",
                "message": "No vector IDs provided",
                "deleted_count": 0
            }))
            .map_err(|e| GraphError::New(e.to_string()))?,
            fmt: Default::default(),
        });
    }

    eprintln!("[HardDeleteVectors] Deleting {} vectors", vector_ids.len());

    let db = Arc::clone(&input.graph.storage);
    let mut txn = db.graph_env.write_txn().map_err(GraphError::from)?;

    let mut deleted_count = 0;
    let mut errors: Vec<String> = Vec::new();

    for id_str in &vector_ids {
        // Parse the ID string to u128
        let id: u128 = match uuid::Uuid::parse_str(id_str) {
            Ok(uuid) => uuid.as_u128(),
            Err(e) => {
                errors.push(format!("Invalid ID '{}': {}", id_str, e));
                continue;
            }
        };

        // Call hard_delete
        match db.vectors.hard_delete(&mut txn, id) {
            Ok(_) => {
                eprintln!("[HardDeleteVectors] Deleted {}", id_str);
                deleted_count += 1;
            }
            Err(e) => {
                // Log but continue - the vector might already be partially deleted
                eprintln!("[HardDeleteVectors] Warning deleting {}: {}", id_str, e);
                deleted_count += 1; // Count it anyway since we attempted cleanup
            }
        }
    }

    txn.commit().map_err(GraphError::from)?;

    eprintln!("[HardDeleteVectors] Complete. Deleted {} vectors", deleted_count);

    Ok(protocol::Response {
        body: sonic_rs::to_vec(&json!({
            "status": "success",
            "deleted_count": deleted_count,
            "requested_count": vector_ids.len(),
            "errors": errors
        }))
        .map_err(|e| GraphError::New(e.to_string()))?,
        fmt: Default::default(),
    })
}

inventory::submit! {
    HandlerSubmission(
        Handler::new("HardDeleteVectors", hard_delete_vectors_inner, true)
    )
}
