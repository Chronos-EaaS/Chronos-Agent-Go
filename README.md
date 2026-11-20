# Chronos Agent for Go

Run your benchmarks with [Chronos Control][control]. The command-line agent picks
up jobs, runs your benchmark commands, and sends logs and results back to
Control. The Go library lets you add the same integration to an existing
benchmark runner.

The agent runs on Linux and macOS. To build or install it from source, you’ll
need Go 1.24 or newer. There’s also a [Java agent][java-agent].

## Getting started

Install the command-line agent with Go.

```sh
go install github.com/Chronos-EaaS/Chronos-Agent-Go/cmd/chronos-agent@main
chronos-agent -help
```

To try it before connecting to Control, clone this repository and run
`./examples/synthetic/demo.sh`. The demo runs the
[included adapter](examples/synthetic/adapter.json) against a temporary local
server.

### Run a benchmark

You’ll need a running Chronos Control instance and a job scheduled for the
system and environment your agent will serve. Create an `adapter.json` file
keyed by the system identifier in Control. This example runs a program called
`run-benchmark` from your `PATH` and passes it the job’s `payload` parameter.

```json
{
  "my-system": {
    "phases": {
      "execute": {
        "program": "run-benchmark",
        "args": ["{{evaluation.payload}}"]
      }
    },
    "result": "metrics.json",
    "artifacts": [{"role": "benchmark-output", "path": "output.txt"}]
  }
}
```

Set `CHRONOS_URL` to your Control server’s URL. If the job is scheduled in
`system-local`, pass `local` as the environment name.

```sh
chronos-agent -url "$CHRONOS_URL" -config adapter.json -environment local -once
```

Leave off `-once` to keep polling for jobs. Give each agent its own queue. If
two agents poll for the same system and environment, both can pick up the same
job. If you use CEM, start the agent with
`-cem-job-id ID` and the matching `-environment` after the CEM bootstrapper
has prepared the job.

### Configure your commands

Each adapter can define commands for `prepare`, `warmup`, `execute`, `analyze`,
and `clean`. The agent runs them in that order, skipping phases disabled in
the job. Use `{{evaluation.name}}` or `{{setup.name}}` as a whole argument to
pass a value from the job’s CDL. The agent passes the value directly to the
program as one argument.

Commands start in a fresh working directory. They can read the full job
configuration from `CHRONOS_CDL_FILE`. If you set `result`, have your command
write a JSON object or array to that file. List any other files you want to
collect under `artifacts`. Both kinds of path are relative to the working
directory. You can also add a `version` command to record the tool’s version
alongside the job.

Control receives the logs and, on success, the result and a ZIP with the
collected files and their SHA-256 checksums. The agent keeps a local copy of
each run under `-work-dir` (default `.chronos-runs`), so you can inspect the
logs even if a command or upload fails. Use `-timeout` to set a time limit for
the job’s commands.

If your Control server sits behind an authentication gateway, set
`CHRONOS_AUTHORIZATION` to the header value your gateway expects. If uploads
go to a separate server, pass its HTTP(S) `/api/v1/job` URL with
`-upload-endpoint`.

### Run a versioned benchmark

If Control has a benchmark binding for `chronos-command@1`, run its jobs with
`-mode attempt-v1`. Put each artifact from the published binding in a trusted
local cache, with its lowercase SHA-256 digest as the filename. The agent checks
the digest and size as it copies each file into a private run directory. Choose
a stable worker name or let it default to the hostname.

```sh
chronos-agent -url "$CHRONOS_URL" -mode attempt-v1 -environment local \
  -artifact-cache /path/to/artifact-cache -worker test-node -once
```

Use the HTTPS URL of your Control server. Use `-supports system-a,system-b` to
limit which systems the worker serves. The same mode can take over a job from
a CEM bootstrapper with `-cem-job-id ID`, the matching `-environment`, and
the bootstrapper’s credential in `CHRONOS_ATTEMPT_TOKEN`.

The staged driver runs directly as a POSIX executable. It receives one JSON
object on stdin with `repetitionId`, `attemptId`, `specification`, and
`artifactPaths`. It uses the frozen specification to prepare and run the
benchmark, including the configured phase exclusions. Write the result object
to stdout and diagnostics to stderr. Exit nonzero if the benchmark fails.
Each output stream has a 1 MiB limit.

The agent keeps the specification and both output streams under `-work-dir`
and uploads them as an attempt ZIP to the same Control origin. Set the server’s
PHP upload limit to accommodate these archives. The attempt also records the
observed host, verified inputs, and driver diagnostics. If the start response
is lost, the agent leaves the claim for an operator to inspect. Operators can
abort and reschedule the repetition to create a new attempt.

### Use the Go library

Add the module to your project.

```sh
go get github.com/Chronos-EaaS/Chronos-Agent-Go@main
```

Import `github.com/Chronos-EaaS/Chronos-Agent-Go` as `agent`. Use
`agent.NewClient` to call the Chronos Control v1 job API from your own runner,
or `agent.NewRunner` to run jobs with the agent’s command adapters. Use
`agent.NewAttemptRunner` for benchmark bindings that use the versioned protocol.

## Contributing

Issues and pull requests are welcome. To run the checks locally, use
`go vet ./...` and `go test -race ./...`.

## License

[MIT License](LICENSE).

[control]: https://github.com/Chronos-EaaS/Chronos-Control
[java-agent]: https://github.com/Chronos-EaaS/Chronos-Agent
