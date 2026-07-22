# Core architecture

Henosis core owns graph lifecycle state, derives plans, dispatches controller work, and folds durable facts into in-memory graph actors.

## Storage

S2 is the sole store of record for graph lifecycle facts. Each graph has one linear stream, and the root `registry` stream records graph registration and retirement. PostgreSQL holds only non-log metadata; it does not copy S2 content.

Every S2 write goes through an append session and carries `match_seq_num`. This optimistic concurrency check is the only cross-process write guard on the S2 seam. An OCC violation poisons the whole append session, so core drops that session at once. An append with an unknown commit result also causes core to drop the session before it reads the authoritative stream.

Recovery always uses a new append session: reload and fold the stream, derive the operation again from that state, then append with the new tail as `match_seq_num`. No token or epoch participates in S2 correctness. The in-memory graph actor cache can be rebuilt from the streams on boot or after a stale write.

Controller reports that only describe current level state remain in memory. Accepted generations, plans, published outputs, stalls, retirement, and other history-shaped facts stay durable in the graph journal.
