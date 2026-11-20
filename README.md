# Chronos Agent for Go

The Go agent runs local benchmark and system commands for
[Chronos Control](https://github.com/Chronos-EaaS/Chronos-Control). The module
contains a v1 client library and a CLI worker. It uses only the Go standard library.
Build with Go 1.24 or later. The runner targets Linux and macOS.

## Build and run

Install the CLI from the main branch.

```sh
go install github.com/Chronos-EaaS/Chronos-Agent-Go/cmd/chronos-agent@main
chronos-agent -help
```

To embed the client in a Go program, add it to your module with
`go get github.com/Chronos-EaaS/Chronos-Agent-Go@main`. Import it as
`agent "github.com/Chronos-EaaS/Chronos-Agent-Go"` and use `agent.NewClient`
for v1 requests or `agent.NewRunner` for local execution.

To build and test from a clone, run the following from the repository root.

```sh
go test -race ./...
go build -o /tmp/chronos-agent ./cmd/chronos-agent
/tmp/chronos-agent -help
./examples/synthetic/demo.sh
```

The demo builds the CLI, starts a synthetic server on a temporary loopback port,
runs the included adapter, and checks the request sequence, uploaded ZIP, digests,
argv, process exit, and empty queue. All demo runs and files use temporary directories.

For a Chronos Control fixture, register the adapter's system identifier and schedule a
job in the matching `system-<environment>` environment. Set
`CHRONOS_FIXTURE_URL` to the URL of your fixture. The bundled adapter expects a
`payload` parameter in the evaluation section. For Chronos Control's seeded
job 90, use a copy of the adapter with `{{evaluation.quantity}}` as the argument.

```sh
PATH="$PWD/examples/synthetic:$PATH" /tmp/chronos-agent \
  -url "$CHRONOS_FIXTURE_URL" -config examples/synthetic/adapter.json \
  -environment agent -work-dir /tmp/chronos-go-runs -once
```

Use one worker per overlapping system and environment queue. The v1 system poll is
a read of a scheduled job. The runner checks the job again before setting it running.
The check and write are separate requests. Concurrent workers may still select the
same job. Omit `-once` to keep polling. The worker exits after an unreconciled error
to allow inspection of the job and its local files. `CHRONOS_AUTHORIZATION` can supply
an Authorization value for a gateway fronting Control. The current v1 endpoints are
public at the application layer. Use the scheme required by the gateway.

## Adapter configuration

The configuration maps Chronos system identifiers to local adapters. Each adapter
declares a subset of the Control lifecycle phases and optional outputs.

```json
{
  "fixture-system": {
    "phases": {
      "execute": {
        "program": "chronos-synthetic-benchmark",
        "args": ["{{evaluation.payload}}"]
      }
    },
    "version": {
      "program": "chronos-synthetic-benchmark",
      "args": ["--version"]
    },
    "result": "metrics.json",
    "artifacts": [{"role": "benchmark-output", "path": "payload.txt"}]
  }
}
```

- Phases run in Control order `prepare`, `warmup`, `execute`, `analyze`, `clean`,
  filtered by the job's phase exclusion bitmask. Each configured phase that
  runs sets its server phase and reports 100% on successful command completion.
  A failed or stopped command ends the phase sequence. A command that acquires
  resources should arrange its own cleanup.
- Programs are absolute paths or bare names on `PATH`, supplied by the local
  operator. A whole argument can bind an exact CDL value using
  `{{evaluation.name}}` or `{{setup.name}}`. Values remain individual arguments,
  including whitespace and shell metacharacters. The runner invokes the configured
  executable directly. Choose arguments and option boundaries appropriate for that
  executable. Arguments that embed values in interpreter code would give that
  interpreter control of the value.
- Commands start in a fresh `work/` directory. The child environment contains
  `CHRONOS_CDL_FILE` pointing to the received XML and whichever of `PATH`, `LANG`,
  `LC_ALL`, `TZ`, and `TMPDIR` are set in the parent. A workspace is a file
  collection boundary. The command runs with the operating system privileges of
  the worker account.
- `version` is an optional local probe with a five-second timeout within the job
  timeout. Its output and process outcome are recorded alongside the phase commands.
- `result` is an optional JSON object or array file, up to 1 MiB. The runner sends
  its original bytes in the PHP form `result` field on success. Configure
  result keys to match the system's Chronos result configuration.
- `artifacts` lists optional-to-configure outputs required for a successful run.
  Output paths reference regular files inside `work/`. The ZIP contains the captured
  bytes, a versioned SHA-256 manifest, logs, and observed execution metadata.
  Manifest URIs are relative to the ZIP root. Files collected from the workload
  and logs have a combined 64 MiB limit. Chronos PHP upload settings can impose
  a lower limit.

## Lifecycle and evidence

The client implements the Chronos Control v1 job API. See the
[v1 PHP controllers](https://github.com/Chronos-EaaS/Chronos-Control/tree/94aae8f360dbadefc05cea3bc654c20823b9b5fa/backend/src/Http/Api/V1)
and [contract tests](https://github.com/Chronos-EaaS/Chronos-Control/blob/94aae8f360dbadefc05cea3bc654c20823b9b5fa/tests/api/agent-contracts.py).
Queue code 601 represents an empty queue. The client tries eligible GET requests
up to three times on transport errors or HTTP 429, 502, 503, and 504. Each request
has a 15-second default HTTP timeout. The runner sends each mutation once. It checks
the current job status before publishing log chunks and again before the terminal
PATCH. The final success PATCH follows successful ZIP upload.

System workers post to `/api/v1/job/id=<id>/action=upload` at the Control origin by
default. For a separate HTTP(S) upload origin, supply its `/api/v1/job` endpoint
explicitly with `-upload-endpoint` and configure it to store uploads for the same
Control instance. The client sends gateway credentials only to the Control origin
and declines redirects. The SDK can inspect `getUploadTarget` metadata. FTP transport
and automatic connection to the discovered target are outside this module.

For CEM, the existing bootstrapper provides the node identity, reserves the job,
performs machine setup, and calls `jobStarted` on the CEM endpoint. Invoke the runner
with `-cem-job-id ID -environment NAME` once the job is in setup. The runner uses the
job API for the workload lifecycle. The bootstrapper owns CEM node telemetry and
termination.

The worker monitors `GET /api/v1/job/id=<id>` during commands. If Control changes the
running status, the runner stops the workload and retains local evidence. The job
timeout reports failure, and local SIGINT or SIGTERM requests abort. Linux and macOS
cancellation kills the process group. Processes that deliberately leave the group
are owned by the operator or bootstrapper. In once and CEM modes, the CLI forwards
ordinary nonzero command exits in the range 1–125, uses 1 for other execution or
publication errors, and uses 130 for local cancellation.

Each run writes a new, private directory containing the received CDL,
`observations.json`, captured `steps.log` when a process was invoked, and the
collected ZIP and manifest where collection succeeds. Output capture retains the
first 256 KiB of each command's stdout and stderr, records truncation, and attaches
the textual log to Control in chunks before completion. Local files from failed
runs and failed publication remain available. Observations include exact argv,
resolved executable path, timestamps, process status, available host and Go
build information, and the CDL digest. Use the version probe to record the
tool or SUT version when the executable can report it. Avoid passing secrets
in CDL arguments or writing secrets to logs or declared artifacts.

The local directory name is a local run identifier. Control v1 stores state and
evidence by job ID. Atomic claims and planned repetition or attempt identities
belong to a future capability-aware integration at the client/runner boundary.
The local process runs once per enabled phase. Read retries cannot add a measurement.

## Checks and license

GitHub Actions runs formatting checks, `go vet`, race-enabled tests, and
the CLI build on pushes and pull requests targeting `main`. The matrix covers
Go 1.24 on Linux and the current stable Go release on Linux and macOS.

[MIT License](LICENSE), under the same notice as Chronos Control.
