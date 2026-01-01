# CSA Coursework: Game of Life skeleton (Go)

All documentation is available [here](https://uob-csa.github.io/gol-docs/)

## Benchmarking Different Configurations

Use the benchmark harness to quickly compare performance across image sizes, thread counts, and execution backends:

```bash
go run ./cmd/bench -sizes 16,64,128,256,512 -threads 1,2,4,8,16 -turns 512 -repeat 5
```