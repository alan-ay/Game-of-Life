package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

type neighborClient struct {
	addr   string
	client *rpc.Client
}

type workerService struct {
	mu             sync.RWMutex
	sessionID      string
	startRow       int
	width          int
	height         int
	slice          [][]byte
	topHalo        []byte
	bottomHalo     []byte
	lastTopEdge    []byte
	lastBottomEdge []byte
	topNeighbor    neighborClient
	bottomNeighbor neighborClient
	localThreads   int
	directHalo     bool
	compressSlice  bool
	completedTurn  int
	aliveCount     int
	stopRequested  bool
	shutdownFn     func() error
}

func (w *workerService) InitSlice(req *gol.WorkerInitSliceRequest, resp *gol.WorkerInitSliceResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}
	if req.Width <= 0 || req.Height <= 0 {
		return fmt.Errorf("invalid slice dimensions")
	}

	slice := make([][]byte, req.Height)
	for i := 0; i < req.Height; i++ {
		row := make([]byte, req.Width)
		copy(row, req.Slice[i])
		slice[i] = row
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.topNeighbor.client != nil {
		w.topNeighbor.client.Close()
	}
	if w.bottomNeighbor.client != nil {
		w.bottomNeighbor.client.Close()
	}

	w.sessionID = req.SessionID
	w.startRow = req.StartRow
	w.width = req.Width
	w.height = req.Height
	w.slice = slice
	w.topHalo = cloneRow(req.TopHalo)
	w.bottomHalo = cloneRow(req.BottomHalo)
	if len(slice) > 0 {
		w.lastTopEdge = cloneRow(slice[0])
		w.lastBottomEdge = cloneRow(slice[len(slice)-1])
	}
	w.directHalo = req.DirectHalo
	w.compressSlice = req.CompressSlice
	w.completedTurn = 0
	w.aliveCount = 0
	w.stopRequested = false
	w.topNeighbor = neighborClient{addr: req.TopNeighbor}
	w.bottomNeighbor = neighborClient{addr: req.BottomNeighbor}

	resp.Assigned = true
	return nil
}

func (w *workerService) Step(req *gol.WorkerStepRequest, resp *gol.WorkerStepResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}

	w.mu.RLock()
	if w.sessionID == "" || req.SessionID != w.sessionID {
		w.mu.RUnlock()
		return fmt.Errorf("session mismatch")
	}
	if w.stopRequested {
		w.mu.RUnlock()
		return fmt.Errorf("session stopped")
	}
	width := w.width
	height := w.height
	currentSlice := cloneWorld(w.slice)
	haloTop := cloneRow(w.topHalo)
	haloBottom := cloneRow(w.bottomHalo)
	startRow := w.startRow
	w.mu.RUnlock()

	if len(currentSlice) == 0 {
		return errors.New("worker slice uninitialised")
	}
	if haloTop == nil || haloBottom == nil {
		return errors.New("halo rows not initialised")
	}

	if !w.directHalo {
		if req.TopHalo != nil {
			haloTop = cloneRow(req.TopHalo)
		}
		if req.BottomHalo != nil {
			haloBottom = cloneRow(req.BottomHalo)
		}
	}

	extended := make([][]byte, height+2)
	extended[0] = haloTop
	for i := 0; i < height; i++ {
		extended[i+1] = currentSlice[i]
	}
	extended[height+1] = haloBottom

	next := make([][]byte, height)
	for i := 0; i < height; i++ {
		next[i] = make([]byte, width)
	}

	threadCount := w.localThreads
	if threadCount < 1 {
		threadCount = 1
	}
	if threadCount > height {
		threadCount = height
	}

	type chunkResult struct {
		alive int
		diffs []gol.CellStateChange
	}
	results := make([]chunkResult, threadCount)

	var wg sync.WaitGroup
	rowsPer := (height + threadCount - 1) / threadCount
	for idx := 0; idx < threadCount; idx++ {
		startY := idx * rowsPer
		endY := startY + rowsPer
		if startY >= height {
			break
		}
		if endY > height {
			endY = height
		}
		wg.Add(1)
		go func(chunk int, from, to int) {
			defer wg.Done()
			localAlive := 0
			var localDiffs []gol.CellStateChange
			if !w.compressSlice {
				localDiffs = make([]gol.CellStateChange, 0, (to-from)*2)
			}
			for y := from; y < to; y++ {
				for x := 0; x < width; x++ {
					aliveNeighbours := 0
					for dy := -1; dy <= 1; dy++ {
						for dx := -1; dx <= 1; dx++ {
							if dx == 0 && dy == 0 {
								continue
							}
							ny := y + 1 + dy
							nx := (x + dx + width) % width
							if extended[ny][nx] == 255 {
								aliveNeighbours++
							}
						}
					}

					cellAlive := extended[y+1][x] == 255
					var newVal byte
					switch {
					case cellAlive && (aliveNeighbours == 2 || aliveNeighbours == 3):
						newVal = 255
					case !cellAlive && aliveNeighbours == 3:
						newVal = 255
					default:
						newVal = 0
					}
					next[y][x] = newVal
					if newVal == 255 {
						localAlive++
					}
					if !w.compressSlice && newVal != currentSlice[y][x] {
						localDiffs = append(localDiffs, gol.CellStateChange{
							X:     x,
							Y:     startRow + y,
							Alive: newVal == 255,
						})
					}
				}
			}
			results[chunk] = chunkResult{alive: localAlive, diffs: localDiffs}
		}(idx, startY, endY)
	}
	wg.Wait()

	var diffs []gol.CellStateChange
	aliveCount := 0
	for _, res := range results {
		aliveCount += res.alive
		if !w.compressSlice {
			diffs = append(diffs, res.diffs...)
		}
	}

	topRow := cloneRow(next[0])
	bottomRow := cloneRow(next[height-1])

	w.mu.Lock()
	w.slice = next
	w.lastTopEdge = topRow
	w.lastBottomEdge = bottomRow
	w.completedTurn = req.Turn
	w.aliveCount = aliveCount
	w.mu.Unlock()

	resp.SessionID = w.sessionID
	resp.StartRow = startRow
	resp.Height = height
	resp.AliveCount = aliveCount
	resp.ReadyForNext = false

	if w.directHalo {
		if err := w.shareHalo(gol.HaloDirectionTop, topRow, req.Turn); err != nil {
			log.Printf("halo exchange (top) failed: %v", err)
			return err
		}
		if err := w.shareHalo(gol.HaloDirectionBottom, bottomRow, req.Turn); err != nil {
			log.Printf("halo exchange (bottom) failed: %v", err)
			return err
		}
		resp.ReadyForNext = true
	} else {
		w.mu.Lock()
		w.topHalo = cloneRow(topRow)
		w.bottomHalo = cloneRow(bottomRow)
		w.mu.Unlock()
		resp.ReadyForNext = true
	}

	if w.compressSlice {
		encoded, err := encodeAndCompressSlice(next)
		if err != nil {
			return err
		}
		resp.EncodedSlice = encoded
		resp.Cells = nil
	} else {
		resp.Cells = diffs
		resp.EncodedSlice = nil
	}
	return nil
}

func (w *workerService) shareHalo(direction gol.HaloDirection, row []byte, turn int) error {
	if row == nil {
		return fmt.Errorf("nil halo row")
	}
	var neighbor neighborClient
	if direction == gol.HaloDirectionTop {
		w.mu.RLock()
		neighbor = w.topNeighbor
		w.mu.RUnlock()
	} else {
		w.mu.RLock()
		neighbor = w.bottomNeighbor
		w.mu.RUnlock()
	}

	if neighbor.addr == "" {
		w.mu.Lock()
		if direction == gol.HaloDirectionTop {
			w.topHalo = cloneRow(row)
		} else {
			w.bottomHalo = cloneRow(row)
		}
		w.mu.Unlock()
		return nil
	}

	client, err := w.dialNeighbor(neighbor.addr)
	if err != nil {
		return err
	}

	var resp gol.HaloExchangeResponse
	req := gol.HaloExchangeRequest{
		SessionID: w.sessionID,
		Turn:      turn,
		Direction: direction,
		Row:       cloneRow(row),
	}
	if err := client.Call("Worker.ExchangeHalo", &req, &resp); err != nil {
		return err
	}

	w.mu.Lock()
	if direction == gol.HaloDirectionTop {
		w.topHalo = cloneRow(resp.Row)
	} else {
		w.bottomHalo = cloneRow(resp.Row)
	}
	w.mu.Unlock()
	return nil
}

func (w *workerService) dialNeighbor(addr string) (*rpc.Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("empty neighbor address")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var target *neighborClient
	if w.topNeighbor.addr == addr {
		target = &w.topNeighbor
	} else if w.bottomNeighbor.addr == addr {
		target = &w.bottomNeighbor
	} else {
		return nil, fmt.Errorf("unknown neighbor %s", addr)
	}

	if target.client != nil {
		return target.client, nil
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	target.client = client
	return client, nil
}

func (w *workerService) ExchangeHalo(req *gol.HaloExchangeRequest, resp *gol.HaloExchangeResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}

	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		w.mu.Lock()
		if w.sessionID != req.SessionID {
			w.mu.Unlock()
			return fmt.Errorf("session mismatch")
		}
		if w.stopRequested {
			w.mu.Unlock()
			return fmt.Errorf("session stopped")
		}
		if w.completedTurn >= req.Turn {
			// Ready to exchange
			break
		}
		w.mu.Unlock()

		select {
		case <-timeout:
			w.mu.RLock()
			stopped := w.stopRequested
			sessionID := w.sessionID
			w.mu.RUnlock()
			if stopped {
				return fmt.Errorf("session stopped while waiting")
			}
			if sessionID != req.SessionID {
				return fmt.Errorf("session changed while waiting")
			}
			return fmt.Errorf("timeout waiting for turn %d completion", req.Turn)
		case <-ticker.C:
			// Continue waiting
		}
	}

	var myEdge []byte
	switch req.Direction {
	case gol.HaloDirectionTop:
		w.bottomHalo = cloneRow(req.Row)
		myEdge = cloneRow(w.lastBottomEdge)
	case gol.HaloDirectionBottom:
		w.topHalo = cloneRow(req.Row)
		myEdge = cloneRow(w.lastTopEdge)
	default:
		w.mu.Unlock()
		return fmt.Errorf("unknown halo direction %v", req.Direction)
	}
	w.mu.Unlock()

	resp.Row = myEdge
	return nil
}

func (w *workerService) AliveStatus(_ *gol.AliveStatusRequest, resp *gol.AliveStatusResponse) error {
	if resp == nil {
		return fmt.Errorf("nil response")
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	resp.CompletedTurns = w.completedTurn
	resp.AliveCount = w.aliveCount
	return nil
}

func (w *workerService) Stop(_ *gol.WorkerStopRequest, _ *gol.WorkerStopResponse) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.stopRequested = true
	w.sessionID = ""
	w.slice = nil
	w.topHalo = nil
	w.bottomHalo = nil
	w.lastTopEdge = nil
	w.lastBottomEdge = nil
	w.completedTurn = 0
	w.aliveCount = 0
	return nil
}

func (w *workerService) Shutdown(_ *gol.WorkerShutdownRequest, _ *gol.WorkerShutdownResponse) error {
	if err := w.Stop(&gol.WorkerStopRequest{}, &gol.WorkerStopResponse{}); err != nil {
		return err
	}
	if w.shutdownFn != nil {
		return w.shutdownFn()
	}
	return nil
}

func cloneRow(row []byte) []byte {
	if row == nil {
		return nil
	}
	dst := make([]byte, len(row))
	copy(dst, row)
	return dst
}

func cloneWorld(world [][]byte) [][]byte {
	if world == nil {
		return nil
	}
	clone := make([][]byte, len(world))
	for i := range world {
		clone[i] = cloneRow(world[i])
	}
	return clone
}

func encodeAndCompressSlice(slice [][]byte) ([]byte, error) {
	packed := packSliceBits(slice)
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(packed); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func packSliceBits(slice [][]byte) []byte {
	if len(slice) == 0 {
		return nil
	}
	width := len(slice[0])
	height := len(slice)
	totalBits := width * height
	if totalBits == 0 {
		return nil
	}
	data := make([]byte, (totalBits+7)/8)
	bitIndex := 0
	for y := 0; y < height; y++ {
		row := slice[y]
		for x := 0; x < width; x++ {
			if row[x] == 255 {
				data[bitIndex>>3] |= 1 << uint(bitIndex&7)
			}
			bitIndex++
		}
	}
	return data
}

func main() {
	addr := flag.String("addr", ":8000", "Address for the GoL worker to listen on")
	flag.Parse()

	server := rpc.NewServer()
	localThreads := envInt("WORKER_LOCAL_THREADS", 1)
	svc := &workerService{localThreads: localThreads}
	if err := server.RegisterName("Worker", svc); err != nil {
		log.Fatalf("register worker service: %v", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	defer listener.Close()

	svc.shutdownFn = func() error {
		return listener.Close()
	}

	log.Printf("GoL worker listening on %s", *addr)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		log.Printf("Received %v, shutting down worker", sig)
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("temporary accept error: %v", err)
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept error: %v", err)
			break
		}
		go server.ServeConn(conn)
	}
}

func envInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	if v, err := strconv.Atoi(val); err == nil && v > 0 {
		return v
	}
	return def
}
