# Design decisions

One record per decision that had real alternatives. Each states what was chosen,
what was rejected, and what the choice costs.

These are dated records, not descriptions of the present. An ADR written at
v0.1.0 talks about v0.3 in the future tense because that is where it was
standing; rewriting them to sound current would turn a history into a claim.
The documents that describe the current state are `README.md` and everything in
`docs/` outside this directory.

| | |
|---|---|
| [0001](0001-storage-concurrency-and-value-representation.md) | Storage, concurrency and value representation |
| [0002](0002-resp2-subset-wire-protocol.md) | A small RESP2 subset instead of a proprietary protocol |
| [0003](0003-fatal-conditions-are-broadcast.md) | Fatal conditions are broadcast, not delivered |
| [0004](0004-canonical-effect-logging.md) | Log canonical effects, not client commands |
| [0005](0005-model-a-durability.md) | Memory becomes visible before the durability acknowledgement |
| [0006](0006-flush-when-the-reader-blocks.md) | Flush a reply when the reader is about to block |
| [0007](0007-what-v1-stabilises.md) | What v1.0 stabilises, and what it does not |
