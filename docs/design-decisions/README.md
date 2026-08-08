# Design decisions

One record per decision that had real alternatives. Each states what was chosen,
what was rejected, and what the choice costs.

| | |
|---|---|
| [0001](0001-storage-concurrency-and-value-representation.md) | Storage, concurrency and value representation |
| [0002](0002-resp2-subset-wire-protocol.md) | A small RESP2 subset instead of a proprietary protocol |
| [0004](0004-canonical-effect-logging.md) | Log canonical effects, not client commands |
| [0005](0005-model-a-durability.md) | Memory becomes visible before the durability acknowledgement |

**There is no 0003, and that is not a lost file.** The design spec names the
persistence decisions 0004 and 0005, and those references are the durable
record. Renumbering to close the gap would break them for no gain, so the gap
stays and this note explains it.
