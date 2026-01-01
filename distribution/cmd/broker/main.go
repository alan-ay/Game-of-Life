package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"uk.ac.bris.cs/gameoflife/gol"
)

type workerBinding struct {
	addr     string
	client   *rpc.Client
	startRow int
	endRow   int
}

type haloPair struct {
	top    []byte
	bottom []byte
}

type sessionState struct {
	id             string
	params         gol.Params
	width          int
	height         int
	world          [][]byte
	completedTurns int
	aliveCount     int
	paused         bool
	workers        []*workerBinding
	mu             sync.Mutex
}

func (s *sessionState) isPaused() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.paused
}

type BrokerService struct {
	mu            sync.RWMutex
	workerAddrs   []string
	workerClients map[string]*rpc.Client
	sessions      map[string]*sessionState
	completedTurn int
	aliveCount    int
	initialised   bool
	shutdownFn    func()
	shutdownOnce  sync.Once
	directHalo    bool
	compressSlice bool
}

func NewBrokerService(workerAddrs []string, directHalo bool, compressSlice bool) *BrokerService {
	return &BrokerService{
		workerAddrs:   workerAddrs,
		workerClients: make(map[string]*rpc.Client),
		sessions:      make(map[string]*sessionState),
		directHalo:    directHalo,
		compressSlice: compressSlice,
	}
}

func (b *BrokerService) SetShutdownHandler(fn func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.shutdownFn = fn
}

func (b *BrokerService) requestShutdown() {
	b.shutdownOnce.Do(func() {
		b.mu.RLock()
		fn := b.shutdownFn
		b.mu.RUnlock()
		if fn != nil {
			fn()
		}
	})
}

func (b *BrokerService) selectPausedSession(resumeID string, force bool) (*sessionState, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if resumeID != "" {
		session, ok := b.sessions[resumeID]
		if !ok {
			return nil, fmt.Errorf("resume session %s not found", resumeID)
		}
		if !session.isPaused() {
			return nil, fmt.Errorf("session %s not paused", resumeID)
		}
		return session, nil
	}
	if !force {
		return nil, nil
	}
	for _, session := range b.sessions {
		if session == nil {
			continue
		}
		if session.isPaused() {
			return session, nil
		}
	}
	return nil, fmt.Errorf("no paused sessions available")
}

func (b *BrokerService) StartSession(req *gol.BrokerStartSessionRequest, resp *gol.BrokerStartSessionResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}
	// Check for resuming a paused session
	if session, err := b.selectPausedSession(req.ResumeID, req.ForceResume); err != nil {
		return err
	} else if session != nil {
		session.mu.Lock()
		session.paused = false
		resp.SessionID = session.id
		resp.Width = session.width
		resp.Height = session.height
		resp.CompletedTurns = session.completedTurns
		resp.AliveCount = session.aliveCount
		resp.World = cloneWorld(session.world)
		session.mu.Unlock()
		return nil
	}

	b.mu.Lock()
	oldSessions := make([]*sessionState, 0, len(b.sessions))
	for _, s := range b.sessions {
		oldSessions = append(oldSessions, s)
	}
	b.mu.Unlock()

	// Stop all old sessions
	for _, s := range oldSessions {
		b.stopSession(s, false)
	}

	if len(req.World) == 0 {
		return fmt.Errorf("empty world")
	}
	width := len(req.World[0])
	height := len(req.World)
	if width == 0 {
		return fmt.Errorf("world has zero width")
	}

	clients, err := b.ensureWorkerClients()
	if err != nil {
		return err
	}
	if len(clients) == 0 {
		return fmt.Errorf("no workers available")
	}

	var wg sync.WaitGroup
	for i, client := range clients {
		wg.Add(1)
		go func(idx int, c *rpc.Client) {
			defer wg.Done()
			var stopResp gol.WorkerStopResponse
			if err := c.Call("Worker.Stop", &gol.WorkerStopRequest{}, &stopResp); err != nil {
				log.Printf("[Broker] Warning: failed to reset worker %s: %v", b.workerAddrs[idx], err)
			}
		}(i, client)
	}
	wg.Wait()

	sessionID := generateSessionID()
	world := cloneWorld(req.World)

	session := &sessionState{
		id:     sessionID,
		params: req.Params,
		width:  width,
		height: height,
		world:  world,
	}

	assignments, err := b.assignWorkers(sessionID, req.Params, world, clients)
	if err != nil {
		return err
	}
	session.workers = assignments

	b.mu.Lock()
	b.sessions[sessionID] = session
	b.mu.Unlock()

	resp.SessionID = sessionID
	resp.Width = width
	resp.Height = height
	resp.CompletedTurns = 0
	resp.AliveCount = gol.CountAliveCells(world)
	resp.World = cloneWorld(world)
	return nil
}

func (b *BrokerService) Step(req *gol.BrokerStepRequest, resp *gol.BrokerStepResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}
	session := b.getSession(req.SessionID)
	if session == nil {
		return fmt.Errorf("unknown session %s", req.SessionID)
	}

	session.mu.Lock()
	if session.paused {
		session.mu.Unlock()
		return fmt.Errorf("session paused")
	}
	turn := session.completedTurns + 1
	workers := make([]*workerBinding, len(session.workers))
	copy(workers, session.workers)
	session.mu.Unlock()

	type workerResult struct {
		encoded      []byte
		cells        []gol.CellStateChange
		startRow     int
		height       int
		aliveCount   int
		readyForNext bool
		err          error
	}

	results := make(chan workerResult, len(workers))
	var wg sync.WaitGroup

	var halos map[string]haloPair
	if !b.directHalo {
		halos = make(map[string]haloPair, len(workers))
		for _, binding := range workers {
			halos[binding.addr] = b.haloForWorker(session, binding)
		}
	}

	for _, binding := range workers {
		binding := binding
		wg.Add(1)
		go func() {
			defer wg.Done()
			var wResp gol.WorkerStepResponse
			stepReq := gol.WorkerStepRequest{
				SessionID: req.SessionID,
				Turn:      turn,
			}
			if !b.directHalo {
				if pair, ok := halos[binding.addr]; ok {
					stepReq.TopHalo = pair.top
					stepReq.BottomHalo = pair.bottom
				}
			}
			err := binding.client.Call("Worker.Step", &stepReq, &wResp)
			if err != nil {
				results <- workerResult{err: fmt.Errorf("worker %s: %w", binding.addr, err)}
				return
			}
			results <- workerResult{
				encoded:      wResp.EncodedSlice,
				cells:        wResp.Cells,
				startRow:     wResp.StartRow,
				height:       wResp.Height,
				aliveCount:   wResp.AliveCount,
				readyForNext: wResp.ReadyForNext,
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	allCells := make([]gol.CellStateChange, 0)
	totalAlive := 0
	updateWithSlice := func(startRow int, data [][]byte) {
		for y := 0; y < len(data); y++ {
			globalY := startRow + y
			if globalY < 0 || globalY >= session.height {
				continue
			}
			rowData := data[y]
			if len(rowData) == 0 {
				continue
			}
			row := session.world[globalY]
			for x := 0; x < session.width && x < len(rowData); x++ {
				newVal := rowData[x]
				if row[x] != newVal {
					allCells = append(allCells, gol.CellStateChange{
						X:     x,
						Y:     globalY,
						Alive: newVal == 255,
					})
					row[x] = newVal
				} else {
					row[x] = newVal
				}
			}
		}
	}

	session.mu.Lock()
	for res := range results {
		if res.err != nil {
			session.mu.Unlock()
			return res.err
		}
		totalAlive += res.aliveCount
		if len(res.encoded) > 0 {
			decoded, err := decodeEncodedSlice(res.encoded, session.width, res.height)
			if err != nil {
				session.mu.Unlock()
				return fmt.Errorf("decode worker slice: %w", err)
			}
			updateWithSlice(res.startRow, decoded)
		} else {
			for _, cell := range res.cells {
				if cell.Y < 0 || cell.Y >= session.height {
					continue
				}
				if cell.X < 0 || cell.X >= session.width {
					continue
				}
				if cell.Alive {
					session.world[cell.Y][cell.X] = 255
				} else {
					session.world[cell.Y][cell.X] = 0
				}
			}
			allCells = append(allCells, res.cells...)
		}
	}
	session.completedTurns = turn
	session.aliveCount = totalAlive
	session.mu.Unlock()

	b.mu.Lock()
	b.completedTurn = turn
	b.aliveCount = totalAlive
	b.mu.Unlock()

	resp.SessionID = session.id
	resp.CompletedTurns = turn
	resp.AliveCount = totalAlive
	resp.Cells = allCells
	return nil
}

func (b *BrokerService) Snapshot(req *gol.BrokerSnapshotRequest, resp *gol.BrokerSnapshotResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}
	session := b.getSession(req.SessionID)
	if session == nil {
		return fmt.Errorf("unknown session %s", req.SessionID)
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	resp.SessionID = session.id
	resp.CompletedTurns = session.completedTurns
	resp.World = cloneWorld(session.world)
	return nil
}

func (b *BrokerService) Command(req *gol.BrokerCommandRequest, resp *gol.BrokerCommandResponse) error {
	if req == nil || resp == nil {
		return fmt.Errorf("nil request or response")
	}
	session := b.getSession(req.SessionID)
	if session == nil {
		return fmt.Errorf("unknown session %s", req.SessionID)
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	switch req.Command {
	case gol.BrokerCommandPause:
		session.paused = true
		resp.State = gol.Paused
		resp.Message = "Paused"
	case gol.BrokerCommandResume:
		session.paused = false
		resp.State = gol.Executing
		resp.Message = "Resumed"
	case gol.BrokerCommandDetach:
		session.paused = true
		resp.State = gol.Paused
		resp.Message = "Detached controller"
	case gol.BrokerCommandShutdown:
		session.paused = true
		resp.State = gol.Quitting
		resp.Message = "Shutdown"
		go func() {
			b.stopSession(session, true)
			b.ShutdownWorkers()
			b.requestShutdown()
		}()
	default:
		return fmt.Errorf("unknown command %s", req.Command)
	}

	resp.SessionID = session.id
	resp.CompletedTurns = session.completedTurns
	resp.AliveCount = session.aliveCount
	return nil
}

func (b *BrokerService) AliveStatus(_ *gol.AliveStatusRequest, resp *gol.AliveStatusResponse) error {
	if resp == nil {
		return fmt.Errorf("nil response")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	resp.CompletedTurns = b.completedTurn
	resp.AliveCount = b.aliveCount
	return nil
}

func (b *BrokerService) stopSession(session *sessionState, terminateWorkers bool) {
	var wg sync.WaitGroup
	for _, binding := range session.workers {
		if binding == nil || binding.client == nil {
			continue
		}
		wg.Add(1)
		go func(client *rpc.Client, terminate bool) {
			defer wg.Done()
			if terminate {
				_ = client.Call("Worker.Shutdown", &gol.WorkerShutdownRequest{}, &gol.WorkerShutdownResponse{})
			} else {
				_ = client.Call("Worker.Stop", &gol.WorkerStopRequest{}, &gol.WorkerStopResponse{})
			}
		}(binding.client, terminateWorkers)
	}
	wg.Wait()

	b.mu.Lock()
	delete(b.sessions, session.id)
	b.mu.Unlock()
}

func (b *BrokerService) getSession(id string) *sessionState {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.sessions[id]
}

func (b *BrokerService) InitWorkers() error {
	b.mu.RLock()
	if b.initialised {
		b.mu.RUnlock()
		return nil
	}
	b.mu.RUnlock()

	clients, err := b.ensureWorkerClients()
	if err != nil {
		return err
	}
	if len(clients) == 0 {
		return fmt.Errorf("no workers available after initialisation")
	}

	b.mu.Lock()
	b.initialised = true
	b.mu.Unlock()
	log.Printf("[Broker] Workers initialised (%d)", len(clients))
	return nil
}

func (b *BrokerService) ShutdownWorkers() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for addr, client := range b.workerClients {
		if client == nil {
			continue
		}
		log.Printf("[Broker] Shutting down worker %s", addr)
		_ = client.Call("Worker.Shutdown", &gol.WorkerShutdownRequest{}, &gol.WorkerShutdownResponse{})
		client.Close()
	}
	b.workerClients = make(map[string]*rpc.Client)
	b.initialised = false
}

func (b *BrokerService) Shutdown(_ *gol.WorkerShutdownRequest, _ *gol.WorkerShutdownResponse) error {
	go func() {
		b.ShutdownWorkers()
		b.requestShutdown()
	}()
	return nil
}

func (b *BrokerService) ensureWorkerClients() ([]*rpc.Client, error) {
	clients := make([]*rpc.Client, 0, len(b.workerAddrs))
	for _, addr := range b.workerAddrs {
		b.mu.RLock()
		client, ok := b.workerClients[addr]
		b.mu.RUnlock()
		if ok && client != nil {
			clients = append(clients, client)
			continue
		}
		newClient, err := b.dialWorkerWithRetry(addr, 5, 2*time.Second)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", addr, err)
		}
		b.mu.Lock()
		b.workerClients[addr] = newClient
		b.mu.Unlock()
		clients = append(clients, newClient)
	}
	return clients, nil
}

func (b *BrokerService) dialWorkerWithRetry(addr string, attempts int, backoff time.Duration) (*rpc.Client, error) {
	var lastErr error
	delay := backoff
	for i := 0; i < attempts; i++ {
		client, err := rpc.Dial("tcp", addr)
		if err == nil {
			return client, nil
		}
		lastErr = err
		time.Sleep(delay)
		delay += backoff
	}
	return nil, lastErr
}

func (b *BrokerService) assignWorkers(sessionID string, params gol.Params, world [][]byte, clients []*rpc.Client) ([]*workerBinding, error) {
	height := len(world)
	if height == 0 {
		return nil, fmt.Errorf("world height is zero")
	}
	width := len(world[0])
	numWorkers := len(clients)
	if numWorkers == 0 {
		return nil, fmt.Errorf("no worker clients")
	}

	activeWorkers := numWorkers
	if activeWorkers > height {
		activeWorkers = height
	}
	if activeWorkers == 0 {
		return nil, fmt.Errorf("insufficient rows for worker assignment")
	}

	activeClients := clients[:activeWorkers]
	activeAddrs := b.workerAddrs[:activeWorkers]

	baseRows := height / activeWorkers
	extraRows := height % activeWorkers
	assignments := make([]*workerBinding, 0, activeWorkers)
	start := 0
	for idx := 0; idx < activeWorkers; idx++ {
		rows := baseRows
		if extraRows > 0 {
			rows++
			extraRows--
		}
		end := start + rows
		if end > height {
			end = height
		}
		slice := cloneWorld(world[start:end])
		topHalo := cloneRow(world[(start-1+height)%height])
		bottomHalo := cloneRow(world[end%height])

		topIdx := (idx - 1 + activeWorkers) % activeWorkers
		bottomIdx := (idx + 1) % activeWorkers

		req := gol.WorkerInitSliceRequest{
			SessionID:      sessionID,
			StartRow:       start,
			Width:          width,
			Height:         end - start,
			Slice:          slice,
			TopHalo:        topHalo,
			BottomHalo:     bottomHalo,
			TopNeighbor:    activeAddrs[topIdx],
			BottomNeighbor: activeAddrs[bottomIdx],
			DirectHalo:     b.directHalo,
			CompressSlice:  b.compressSlice,
		}
		var resp gol.WorkerInitSliceResponse
		if err := activeClients[idx].Call("Worker.InitSlice", &req, &resp); err != nil {
			return nil, fmt.Errorf("init slice worker %s: %w", activeAddrs[idx], err)
		}
		assignments = append(assignments, &workerBinding{
			addr:     activeAddrs[idx],
			client:   activeClients[idx],
			startRow: start,
			endRow:   end,
		})
		start = end
	}
	return assignments, nil
}

func (b *BrokerService) haloForWorker(session *sessionState, binding *workerBinding) haloPair {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.world == nil || session.height == 0 {
		return haloPair{}
	}
	topIdx := binding.startRow - 1
	if topIdx < 0 {
		topIdx += session.height
	}
	bottomIdx := binding.endRow % session.height
	top := cloneRow(session.world[topIdx])
	bottom := cloneRow(session.world[bottomIdx])
	return haloPair{top: top, bottom: bottom}
}

func cloneWorld(src [][]byte) [][]byte {
	if src == nil {
		return nil
	}
	dst := make([][]byte, len(src))
	for i := range src {
		if src[i] == nil {
			continue
		}
		row := make([]byte, len(src[i]))
		copy(row, src[i])
		dst[i] = row
	}
	return dst
}

func cloneRow(row []byte) []byte {
	if row == nil {
		return nil
	}
	dst := make([]byte, len(row))
	copy(dst, row)
	return dst
}

func generateSessionID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func main() {
	addr := flag.String("addr", ":7000", "Address for the broker to listen on")
	workers := flag.String("workers", "", "Comma-separated list of worker addresses (overrides env)")
	workersEnv := flag.String("workers-env", ".env", "Path to .env file containing WORKER_ADDRS entry")
	flag.Parse()

	var addresses []string
	directHalo := true
	compressSlice := true
	if *workers != "" {
		addresses = parseWorkerAddresses(*workers)
	} else {
		var err error
		addresses, directHalo, compressSlice, err = loadWorkerConfig(*workersEnv)
		if err != nil {
			log.Fatalf("load worker addresses from %s: %v", *workersEnv, err)
		}
	}
	if len(addresses) == 0 {
		log.Fatal("no worker addresses specified")
	}

	server := rpc.NewServer()
	svc := NewBrokerService(addresses, directHalo, compressSlice)
	if err := server.RegisterName("Broker", svc); err != nil {
		log.Fatalf("register broker service: %v", err)
	}

	if err := svc.InitWorkers(); err != nil {
		log.Fatalf("initialise workers: %v", err)
	}
	defer svc.ShutdownWorkers()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}
	defer listener.Close()

	svc.SetShutdownHandler(func() {
		log.Printf("[Broker] Shutdown requested, closing listener")
		_ = listener.Close()
	})

	log.Printf("Broker listening on %s", *addr)
	log.Printf("Workers: %v", addresses)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-quit
		log.Printf("Received %v, shutting down broker", sig)
		_ = listener.Close()
		svc.ShutdownWorkers()
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

func parseWorkerAddresses(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			addresses = append(addresses, part)
		}
	}
	return addresses
}

func loadWorkerConfig(path string) ([]string, bool, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, true, true, fmt.Errorf("open env file %s: %w", path, err)
	}
	defer file.Close()

	var raw string
	directHalo := true
	compressSlice := true
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(line[len("export "):])
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') || (val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		switch key {
		case "WORKER_ADDRS":
			raw = val
		case "WORKER_DIRECT_HALO":
			if parsed, err := strconv.ParseBool(val); err == nil {
				directHalo = parsed
			}
		case "WORKER_COMPRESS_SLICE":
			if parsed, err := strconv.ParseBool(val); err == nil {
				compressSlice = parsed
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, true, true, fmt.Errorf("read env file %s: %w", path, err)
	}
	if raw == "" {
		return nil, true, true, fmt.Errorf("WORKER_ADDRS not found in %s", path)
	}
	addresses := parseWorkerAddresses(raw)
	if len(addresses) == 0 {
		return nil, true, true, fmt.Errorf("WORKER_ADDRS in %s produced no addresses", path)
	}
	return addresses, directHalo, compressSlice, nil
}

func decodeEncodedSlice(encoded []byte, width, height int) ([][]byte, error) {
	data, err := decompressData(encoded)
	if err != nil {
		return nil, err
	}
	return unpackSliceBits(data, width, height)
}

func decompressData(encoded []byte) ([]byte, error) {
	if len(encoded) == 0 {
		return nil, fmt.Errorf("empty encoded data")
	}
	reader := bytes.NewReader(encoded)
	zr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func unpackSliceBits(bits []byte, width, height int) ([][]byte, error) {
	totalBits := width * height
	expectedBytes := (totalBits + 7) / 8
	if len(bits) < expectedBytes {
		return nil, fmt.Errorf("encoded data too short")
	}
	slice := make([][]byte, height)
	bitIndex := 0
	for y := 0; y < height; y++ {
		row := make([]byte, width)
		for x := 0; x < width; x++ {
			if bits[bitIndex>>3]&(1<<uint(bitIndex&7)) != 0 {
				row[x] = 255
			} else {
				row[x] = 0
			}
			bitIndex++
		}
		slice[y] = row
	}
	return slice, nil
}
