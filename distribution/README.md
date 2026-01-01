# CSA Coursework: Game of Life skeleton (Go)

All documentation is available [here](https://uob-csa.github.io/gol-docs/)

## Distributed Deployment Flow

1. Copy `.env.example` to `.env` and update the ordered `WORKER_ADDRS`, SSH credentials, and remote workspace path.
2. Run `scripts/distribute-workers.sh` to bundle the worker code and push it to every worker host under `~/workspace/golworker`.
3. Log into each worker machine and run `~/workspace/golworker/run-worker.sh`. 
4. Start the broker with `./scripts/start-broker.sh --env ./.env --addr :8080`. 
5. Launch the controller via `./scripts/start-controller.sh -b localhost:8080` (add `--headless` if you do not need the SDL window).

## Benchmarking Different Configurations

Use the benchmark harness to quickly compare performance across image sizes, thread counts, and execution backends:

```bash
go run ./cmd/bench -sizes 64,128,256 -turns 256 -repeat 3 
```
