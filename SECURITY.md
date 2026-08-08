# Security

## Status

**This project is not production-ready.** It is built to explore storage and
networking internals. Do not expose it to an untrusted network.

## Known limitations

- No authentication and no TLS. Anyone who can reach the port has full access.
- No per-client memory accounting; a client may hold a large output buffer.
- The dataset must fit in memory; there is no eviction policy.
- Frame size limits and a client limit exist and are configurable, but they have
  not been audited against a determined attacker.

## Reporting

Open a GitHub issue for anything that is not sensitive. For anything you would
rather not disclose publicly, use GitHub's private vulnerability reporting on
this repository.
