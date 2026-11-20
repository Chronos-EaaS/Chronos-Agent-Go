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

### Use the Go library

Add the module to your project.

```sh
go get github.com/Chronos-EaaS/Chronos-Agent-Go@main
```

Import `github.com/Chronos-EaaS/Chronos-Agent-Go` as `agent`. Use
`agent.NewClient` to call the Chronos Control v1 job API from your own runner,
or `agent.NewRunner` to run jobs with the agent’s command adapters.

## Contributing

Issues and pull requests are welcome. To run the checks locally, use
`go vet ./...` and `go test -race ./...`.

## License

[MIT License](LICENSE).

[control]: https://github.com/Chronos-EaaS/Chronos-Control
[java-agent]: https://github.com/Chronos-EaaS/Chronos-Agent
