# Transaction Epoch Shifting (TES) Optimization

This document describes the design intent and adoption guidelines for the Transaction Epoch Shifting (TES) scheduling optimization in LineairDB.

## 1. Design Intent

The primary goal of Transaction Epoch Shifting (TES) is to enhance write throughput under epoch-based group commit (durability) mechanisms such as Write-Ahead Logging (WAL) and checkpointing (CPR).

By buffering write-heavy transactions and shifting their execution to later epochs:
- **Write Consolidation**: TES groups updates to overlapping keys into identical epochs, reducing the overhead of log persistence and metadata management.
- **Worker-Local Conflict Reduction**: Worker-local Bloom filters (Dirty Summaries) prevent overlapping writes from being shifted concurrently, avoiding dependency cascades.
- **OCC Optimization**: Restricting shiftable transactions to those with small read sets mitigates the risk of optimistic concurrency control (OCC) validation aborts during deferred execution.

---

## 2. Adoption & Tuning Guidelines

TES is highly effective when paired with the right database configurations, but should be disabled in others.

### Concurrency Control Protocols
- **Silo / SiloNWR**: TES is compatible and highly recommended for write-heavy workloads under Silo-based protocols.
- **2PL (Two-Phase Locking)**: **DO NOT enable TES under 2PL mixed workloads.** Deferring transaction completion stretches the duration that lock resources are held. This leads to severe cascading lock contention, spiking the transaction abort rate (>90%) and significantly degrading throughput.

### Durability Modes
- **WAL (Write-Ahead Logging) & CPR (Checkpointing)**: Highly effective. Grouping transactions maximizes epoch-based batching.
- **Memory-only / No durability**: Neutral. In the absence of I/O synchronization barriers, the scheduling overhead of TES outweighs the scheduling benefits.

### Parameter Tuning
- `tes.latency_bound` (K): Recommended value is `5`. Setting this bound too high (e.g., `>= 10`) under high contention causes queueing cascades and raises tail latency.
- `tes.warmup_count` (W): Recommended value is `64` to allow the worker-local Bloom filters to stabilize before shifting starts.
- `tes.max_read_set_size`: Recommended value is `8`. Large read sets increase OCC validation failure probability during shifting.
