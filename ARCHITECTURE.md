# Core architecture

Henosis core owns graph lifecycle state, derives plans, dispatches controller work, and folds durable facts into in-memory graph actors.

## Storage

S2 is the sole store of record for graph lifecycle facts. Each graph has one linear stream, and the root `registry` stream records graph registration and retirement. PostgreSQL holds only non-log metadata; it does not copy S2 content.

Every S2 write goes through an append session and carries `match_seq_num`. This optimistic concurrency check is the only cross-process write guard on the S2 seam. An OCC violation poisons the whole append session, so core drops that session at once. An append with an unknown commit result also causes core to drop the session before it reads the authoritative stream.

Recovery always uses a new append session: reload and fold the stream, derive the operation again from that state, then append with the new tail as `match_seq_num`. No token or epoch participates in S2 correctness. The in-memory graph actor cache can be rebuilt from the streams on boot or after a stale write.

Controller reports that only describe current level state remain in memory. Accepted generations, plans, published outputs, stalls, retirement, and other history-shaped facts stay durable in the graph journal.

## Distributed core model

Core instances share one S2 basin, one content-addressed bundle store, and the same target systems. No process owns a graph. An actor and its controller lanes only cache work for one process.

Before an actor derives a command or serves a graph snapshot, it compares its cached graph-stream tail with S2. If a peer advanced the stream, it reloads and folds the full stream, rebuilds its runtime bindings, and derives again. Every durable transition then appends at the captured tail. An OCC loser drops its poisoned session, reloads, and repeats the derivation on a fresh session. The registry stream applies the same rule to graph creation and retirement.

Controller work is level-triggered and may run on more than one instance. Correctness therefore comes from target observation, ownership metadata, and target-side write preconditions, not from the per-process lane scheduler. Controller output publications carry a generation and deterministic publication ID; the graph-stream append decides which derived transition becomes durable. Old-generation reports cannot satisfy a newer plan.

Bundle directories are addressed by their verified digest. Writers build a complete directory under the shared root and publish it with one rename. A concurrent writer either publishes first or verifies the directory that won; readers never rely on a partly written destination.

Graph watch channels and controller disposition caches are per-instance level views. A reconnect starts from that instance's current S2-backed snapshot; watch sequence numbers have no cross-instance meaning. `GetGraph` refreshes a known actor from S2 before replying. `ListGraphs` and discovery of graphs created after an instance boot still depend on adding a live registry follower; callers must treat the current list as a stale-tolerant view until that work lands.

The target seam must reject stale mutation. The Git/Kubernetes path uses observed-directory equality plus a branch lease, and the Supabase path uses database-side observation digests. Cloudflare's APIs do not expose one atomic precondition across Workers, tunnels, and routes; its observe-again check narrows the race but does not prove stale-delete safety. That provider gap needs an explicit design decision rather than an in-process lock.
