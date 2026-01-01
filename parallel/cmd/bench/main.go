package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

type execBackend string

const (
	backendAuto        execBackend = "auto"
	backendParallel    execBackend = "parallel"
	backendDistributed execBackend = "distributed"
)

type sizeSpec struct {
	width  int
	height int
	label  string
}

type benchResult struct {
	size      sizeSpec
	threads   int
	turns     int
	backend   execBackend
	durations []time.Duration
}

func main() {
	var (
		sizeFlag    = flag.String("sizes", "16,64,128,256,512", "Comma separated list of sizes (either N or WxH).")
		threadFlag  = flag.String("threads", "1,2,4,8", "Comma separated list of worker thread counts.")
		turnsFlag   = flag.Int("turns", 128, "Number of turns per benchmark run.")
		repeatFlag  = flag.Int("repeat", 3, "Number of repetitions per configuration.")
		backendFlag = flag.String("backends", "auto", "Comma separated list of backends: auto, parallel, distributed.")
		workerFlag  = flag.String("worker", "localhost:8080", "Broker address for distributed runs.")
		silentFlag  = flag.Bool("quiet", false, "Reduce per-run logging.")
	)
	flag.Parse()

	sizes, err := parseSizes(*sizeFlag)
	if err != nil {
		log.Fatalf("failed to parse sizes: %v", err)
	}
	if len(sizes) == 0 {
		log.Fatalf("no valid sizes provided")
	}
	threads, err := parseInts(*threadFlag)
	if err != nil {
		log.Fatalf("failed to parse threads: %v", err)
	}
	if len(threads) == 0 {
		log.Fatalf("no valid thread counts provided")
	}
	if *turnsFlag <= 0 {
		log.Fatalf("turns must be positive")
	}
	if *repeatFlag <= 0 {
		log.Fatalf("repeat must be positive")
	}
	backends, err := parseBackends(*backendFlag)
	if err != nil {
		log.Fatalf("failed to parse backends: %v", err)
	}
	if len(backends) == 0 {
		log.Fatalf("no valid backends selected")
	}

	supportsDistributed := paramsHasField("WorkerAddress")
	supportsParallel := !supportsDistributed

	var resolvedBackends []execBackend
	for _, b := range backends {
		switch b {
		case backendAuto:
			if supportsDistributed {
				resolvedBackends = append(resolvedBackends, backendDistributed)
			} else {
				resolvedBackends = append(resolvedBackends, backendParallel)
			}
		case backendDistributed:
			if !supportsDistributed {
				log.Printf("[bench] skipping distributed backend: current codebase does not support remote workers")
				continue
			}
			resolvedBackends = append(resolvedBackends, b)
		case backendParallel:
			if !supportsParallel {
				log.Printf("[bench] skipping parallel backend: current codebase only supports distributed execution")
				continue
			}
			resolvedBackends = append(resolvedBackends, b)
		default:
			log.Printf("[bench] unknown backend %q skipped", string(b))
		}
	}
	if len(resolvedBackends) == 0 {
		log.Fatalf("no runnable backends after compatibility checks")
	}

	var allResults []benchResult
	for _, backend := range resolvedBackends {
		log.Printf("[bench] running backend: %s", backend)
		for _, size := range sizes {
			for _, threadCount := range threads {
				cfgResult := benchResult{
					size:      size,
					threads:   threadCount,
					turns:     *turnsFlag,
					backend:   backend,
					durations: make([]time.Duration, 0, *repeatFlag),
				}
				for iter := 0; iter < *repeatFlag; iter++ {
					params := gol.Params{
						Turns:       *turnsFlag,
						Threads:     threadCount,
						ImageWidth:  size.width,
						ImageHeight: size.height,
					}
					if backend == backendDistributed {
						setWorkerAddress(&params, *workerFlag)
					}
					runLabel := fmt.Sprintf("%s threads=%d turns=%d run=%d/%d backend=%s",
						size.label, threadCount, *turnsFlag, iter+1, *repeatFlag, backend)
					if !*silentFlag {
						log.Printf("[bench] start %s", runLabel)
					}
					duration, runErr := runOnce(params)
					if runErr != nil {
						log.Fatalf("run failed for %s: %v", runLabel, runErr)
					}
					cfgResult.durations = append(cfgResult.durations, duration)
					if !*silentFlag {
						log.Printf("[bench] done  %s -> %.3fs", runLabel, duration.Seconds())
					}
				}
				allResults = append(allResults, cfgResult)
			}
		}
	}

	printSummary(allResults)
}

func runOnce(params gol.Params) (time.Duration, error) {
	events := make(chan gol.Event, 1024)
	var drain sync.WaitGroup
	drain.Add(1)
	go func() {
		defer drain.Done()
		for range events {
		}
	}()

	start := time.Now()
	var runErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("gol.Run panic: %v", r)
			}
		}()
		gol.Run(params, events, nil)
	}()
	elapsed := time.Since(start)
	drain.Wait()
	return elapsed, runErr
}

func parseSizes(raw string) ([]sizeSpec, error) {
	var sizes []sizeSpec
	entries := strings.Split(raw, ",")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		width, height, err := parseSize(entry)
		if err != nil {
			return nil, err
		}
		label := fmt.Sprintf("%dx%d", width, height)
		sizes = append(sizes, sizeSpec{width: width, height: height, label: label})
	}
	return sizes, nil
}

func parseSize(value string) (int, int, error) {
	if strings.ContainsAny(value, "xX") {
		parts := strings.FieldsFunc(value, func(r rune) bool {
			return r == 'x' || r == 'X'
		})
		if len(parts) != 2 {
			return 0, 0, fmt.Errorf("invalid size %q", value)
		}
		w, err := parsePositive(parts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid width in %q: %w", value, err)
		}
		h, err := parsePositive(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid height in %q: %w", value, err)
		}
		return w, h, nil
	}
	n, err := parsePositive(value)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid size %q: %w", value, err)
	}
	return n, n, nil
}

func parsePositive(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, errors.New("empty")
	}
	var value int
	_, err := fmt.Sscanf(raw, "%d", &value)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("expected positive integer, got %q", raw)
	}
	return value, nil
}

func parseInts(raw string) ([]int, error) {
	var values []int
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		value, err := parsePositive(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid int %q: %w", entry, err)
		}
		values = append(values, value)
	}
	return values, nil
}

func parseBackends(raw string) ([]execBackend, error) {
	var backends []execBackend
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(strings.ToLower(entry))
		if entry == "" {
			continue
		}
		switch execBackend(entry) {
		case backendAuto, backendParallel, backendDistributed:
			backends = append(backends, execBackend(entry))
		default:
			return nil, fmt.Errorf("unknown backend %q", entry)
		}
	}
	return backends, nil
}

func paramsHasField(name string) bool {
	var params gol.Params
	typ := reflect.TypeOf(params)
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	_, ok := typ.FieldByName(name)
	return ok
}

func setWorkerAddress(p *gol.Params, addr string) {
	rv := reflect.ValueOf(p).Elem()
	field := rv.FieldByName("WorkerAddress")
	if !field.IsValid() || !field.CanSet() || field.Kind() != reflect.String {
		return
	}
	field.SetString(addr)
}

func printSummary(results []benchResult) {
	if len(results) == 0 {
		fmt.Println("no results to report")
		return
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].backend != results[j].backend {
			return results[i].backend < results[j].backend
		}
		if results[i].size.label != results[j].size.label {
			return results[i].size.label < results[j].size.label
		}
		return results[i].threads < results[j].threads
	})

	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" backend   size      threads  turns  avg(s)  best(s)  stddev(ms)  cells/s (M)")
	fmt.Println("--------------------------------------------------------------------------------")
	for _, res := range results {
		if len(res.durations) == 0 {
			continue
		}
		avg, best, stddev := summarise(res.durations)
		cellOps := float64(res.size.width * res.size.height * res.turns)
		throughput := (cellOps / 1e6) / avg.Seconds()
		fmt.Printf(" %-9s %-8s %8d %6d %7.3f %8.3f %11.3f %11.3f\n",
			res.backend,
			res.size.label,
			res.threads,
			res.turns,
			avg.Seconds(),
			best.Seconds(),
			stddev.Seconds()*1000,
			throughput,
		)
	}
	fmt.Println("--------------------------------------------------------------------------------")
}

func summarise(values []time.Duration) (avg time.Duration, best time.Duration, stddev time.Duration) {
	if len(values) == 0 {
		return 0, 0, 0
	}
	sum := 0.0
	best = values[0]
	for _, v := range values {
		sum += v.Seconds()
		if v < best {
			best = v
		}
	}
	avgSeconds := sum / float64(len(values))
	varianceSum := 0.0
	for _, v := range values {
		diff := v.Seconds() - avgSeconds
		varianceSum += diff * diff
	}
	variance := 0.0
	if len(values) > 1 {
		variance = varianceSum / float64(len(values)-1)
	}
	stddevSeconds := math.Sqrt(variance)
	return time.Duration(avgSeconds * float64(time.Second)), best, time.Duration(stddevSeconds * float64(time.Second))
}
