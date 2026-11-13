# Deploying Fusee

- [`systemd/fusee.service`](systemd/fusee.service): one server on a host,
  with the token, the HTTP API on :7601 and hardening. Instructions in the
  file.
- [`helm/fusee`](helm/fusee): a StatefulSet. `members: 1` is one server;
  `members: 3` is a replicated group (each member keeps a copy; a majority
  must be up). The chart generates the token into a Secret unless you set
  one. `helm install fusee deploy/helm/fusee --set members=3`.
- [`terraform`](terraform): installs the chart with Terraform (Helm
  provider), generates the token, and outputs the addresses.
- [`monitoring`](monitoring): Prometheus alert rules and a Grafana dashboard
  over `/metrics`.
- The [`Dockerfile`](../Dockerfile) at the root builds the server image.

Put the data directory on a disk that honours flushes; `fusee doctor <dir>`
checks it. A Python worker connects with
`Engine.open(address=fusee.connection_url(addr, token_file=path))` and a
client with `Client.connect(addr, token_file=path)` (see each SDK's README);
tools use the HTTP API with the token as `Authorization: Bearer`.

## Who can reach what

`fusee server` serves its own protocol (workers and clients, `--listen`)
and, with `--http host:port`, the HTTP API, the UI and `/metrics`. Each
token is a file; the HTTP API takes it as `Authorization: Bearer <token>`,
workers and clients as `?token-file=PATH` in the address.

| Token | Flag | Server protocol | HTTP API |
| --- | --- | --- | --- |
| worker | `--token-file FILE` | holds shards, any call | everything |
| client | `--client-token-file FILE` | calls (start, signal, query, ...), no shards | everything |
| read-only | `--read-token-file FILE` | calls that read (list, describe, query, result, ...) | every `GET`; a change answers 403 |
| a namespace's | `--namespaces-file FILE` | | runs whose ID starts with `<namespace>/`; schedules, objects and metrics need a full token |

Behind a proxy that signs people in (oauth2-proxy, an identity-aware load
balancer), pass `--identity-header X-Forwarded-User` (and
`--http-insecure` if the API listens off loopback with no token): calls
then need that header or a token, the UI shows who is signed in, and every
signal, cancellation, termination and reset records the user
([audit](../docs/features.md#audit)). Only the proxy may reach the API.

Server-protocol tokens are at least 16 bytes (`openssl rand -hex 32`). A
client or read-only token needs the worker token too. The scope is fixed
when a connection is made: a client's token is refused at the worker's
hello, and a reader's calls that change something answer `Rejected`.

`kill -HUP <pid>` reloads the certificates and every token file (and the
namespaces file) for connections made after it: write the new token, send
`SIGHUP`, then move the callers over. A file that does not load keeps the
old settings and the server says why. The replica protocol's certificates
and token need a restart.

A namespace is a prefix of the workflow ID. The file holds one namespace and
its token per line:

```
# namespace token
acme   9c1f0e...
globex 47ab2d...
```

With `acme`'s token, starting `acme/order-1` works and starting or reading
`globex/order-1` answers 403; `GET /v1/runs` lists only `acme/` runs, and
`/v1/info` names the namespace.

Payloads the application encrypted show as *encrypted* in the UI; to read
them there, run `fusee codec-server` where the keys are and pass its URL as
`--codec-url` ([payload encryption](../docs/features.md#payload-encryption)).
