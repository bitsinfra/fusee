# Running it

The same workflow code runs in five arrangements. Start with the first
and move when you need what the next one gives.

| | Where the journal lives | Code runs in | When |
| --- | --- | --- | --- |
| Embedded | a directory on the app's disk | the app | one process, or one per directory |
| Server | `fusee server`'s disk | the app (workers) | several processes, one store |
| Replicated | three or five servers | the app (workers) | surviving a lost machine |
| Invoked | a server | functions it calls (Lambda, Cloud Run, Vercel) | nothing running while runs sleep |
| Kubernetes | a StatefulSet of servers | the app's pods | the above, managed |

## Embedded

```ts
const engine = await Engine.open({ dir: "./data", workflows: [checkout] });
```

The engine is a library: no server, no network. A run's records go to the
directory's log with one flush per batch; a crash loses nothing that was
acknowledged. One process opens a directory at a time (a second one is
told which process holds it). Use [durability modes](../durability.md) to
choose the flush (`fsync`, `fullfsync` on macOS) and memory for tests.

## Server

```sh
fusee server --dir /var/lib/fusee --listen 0.0.0.0:7600 --token-file /etc/fusee/token --http 0.0.0.0:7601
```

```ts
const engine = await Engine.open({ durability: "server", address: "fusee.internal:7600?token-file=/etc/fusee/token", workflows: [checkout] });
```

Workers lease shards from the server and run their runs; when a worker
dies its shards go to another. Clients (`Client.connect`, the CLI, the HTTP
API) reach any run through the server. Tokens and scopes: [deploying](../../deploy/README.md#who-can-reach-what).
Details: [server mode](../server-mode.md).

## Replicated

Three or five servers, each with its own disk, agree on every record
before it is acknowledged; a majority must be up. Workers connect to the
group as to one server and follow its primary. [Replication](../replication.md)
covers setup, membership changes and what is measured.

## Invoked

The server calls an HTTP endpoint when a shard has work and no worker:
the function opens an engine for that shard, runs it until idle, and
returns. Nothing runs while workflows sleep for a week.

```sh
fusee server --dir /var/lib/fusee --listen 0.0.0.0:7600 --token-file /etc/fusee/token \
  --invoke https://worker.example.com/invoke --invoke-secret-file /etc/fusee/invoke-secret
```

Each SDK has a handler that verifies the request and runs the shard
([server mode, invoked workers](../server-mode.md#invoked-workers)).

## Kubernetes

`helm install fusee deploy/helm/fusee --set members=3` runs a replicated
group as a StatefulSet with its token in a Secret; `deploy/terraform` does
the same through Terraform, `deploy/systemd` runs one server on a host,
and `deploy/monitoring` has alert rules and a dashboard over `/metrics`.
See [deploying](../../deploy/README.md).

## Watching it

`/metrics` (Prometheus), the UI on the `--http` address, `fusee runs ls
--status stuck`, and the [operations](../operations.md) page: disk checks,
backups, upgrades, traces.
