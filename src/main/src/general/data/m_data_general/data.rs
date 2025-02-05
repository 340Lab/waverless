/// Data Interface for Distributed Storage
///
/// # Design Overview
/// The data interface provides a general-purpose solution for distributed data storage
/// and retrieval. It implements a shard-based approach that differs from the batch
/// interface in its core design:
///
/// ## Data Interface
/// - Purpose: General-purpose data read/write operations
/// - Write Process:
///   * Data is sharded according to distribution strategy
///   * Shards are distributed to different nodes
///   * Each node stores its assigned shards
///   * Metadata is updated after all writes complete
/// - Read Process:
///   * Metadata is retrieved to locate shards
///   * Shards are collected from respective nodes
///   * Complete data is reassembled from shards
///
/// ## Comparison with Batch Interface
/// While the batch interface (see batch.rs) focuses on efficient streaming transfer
/// from data holders, the data interface:
/// - Ensures data consistency across nodes
/// - Provides random access to data
/// - Supports complex distribution strategies
/// - Maintains complete metadata for all operations
///
/// # Implementation Details
/// This interface implements:
/// - Distributed shard management
/// - Concurrent read/write operations
/// - Metadata synchronization
/// - Data consistency verification
///
/// For streaming transfer functionality, see the batch.rs module.
use super::*;
// ... existing code ... 