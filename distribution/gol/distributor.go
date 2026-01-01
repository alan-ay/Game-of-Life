package gol

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type distributorChannels struct {
	events     chan<- Event
	ioCommand  chan<- ioCommand
	ioIdle     <-chan bool
	ioFilename chan<- string
	ioOutput   chan<- uint8
	ioInput    <-chan uint8
}

// distributor manages IO, events, and per-turn orchestration.
func distributor(p Params, c distributorChannels, keyPresses <-chan rune) {
	width, height := p.ImageWidth, p.ImageHeight

	world := make([][]byte, height)
	for y := 0; y < height; y++ {
		world[y] = make([]byte, width)
	}

	c.ioCommand <- ioInput
	c.ioFilename <- fmt.Sprintf("%vx%v", width, height)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			world[y][x] = <-c.ioInput
		}
	}

	var currentTurn struct {
		sync.Mutex
		value int
	}
	var currentAlive struct {
		sync.Mutex
		value int
	}
	currentTurn.value = 0
	currentAlive.value = 0

	ticker := time.NewTicker(2 * time.Second)
	tickerStop := make(chan struct{})
	var tickerWG sync.WaitGroup
	tickerWG.Add(1)
	go func() {
		defer tickerWG.Done()
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				currentTurn.Lock()
				completed := int(currentTurn.value)
				currentTurn.Unlock()
				currentAlive.Lock()
				alive := int(currentAlive.value)
				currentAlive.Unlock()
				c.events <- AliveCellsCount{CompletedTurns: completed, CellsCount: alive}
			case <-tickerStop:
				return
			}
		}
	}()

	if p.WorkerAddress == "" {
		log.Fatal("[Distributor] Worker address must be provided for distributed execution")
	}

	var remote *remoteSession

	rpcClient, err := rpc.Dial("tcp", p.WorkerAddress)
	if err != nil {
		log.Fatalf("[Distributor] Failed to connect to broker at %s: %v", p.WorkerAddress, err)
	}
	client := rpcClient
	defer client.Close()

	var statusResp AliveStatusResponse
	if err := client.Call("Broker.AliveStatus", &AliveStatusRequest{}, &statusResp); err != nil {
		log.Fatalf("[Distributor] Remote endpoint is not a broker (%v)", err)
	}

	log.Printf("[Distributor] Connected to Broker at %s", p.WorkerAddress)
	remote, startResp, err := startRemoteSession(client, p, world)
	if err != nil {
		log.Fatalf("[Distributor] Failed to start session: %v", err)
	}

	world = cloneWorld(startResp.World)
	turnLimit := p.Turns
	turn := startResp.CompletedTurns
	paused := false
	quitting := false
	shutdownRequested := false

	currentTurn.Lock()
	currentTurn.value = turn
	currentTurn.Unlock()
	currentAlive.Lock()
	currentAlive.value = startResp.AliveCount
	currentAlive.Unlock()

	aliveSnapshot := collectAliveCells(world)
	if len(aliveSnapshot) > 0 {
		cells := make([]util.Cell, len(aliveSnapshot))
		copy(cells, aliveSnapshot)
		c.events <- CellsFlipped{CompletedTurns: turn, Cells: cells}
	}
	currentAlive.Lock()
	currentAlive.value = len(aliveSnapshot)
	currentAlive.Unlock()
	c.events <- StateChange{CompletedTurns: turn, NewState: Executing}

	handleKey := func(key rune) {
		switch key {
		case 'p':
			if paused {
				paused = false
				log.Printf("Continuing")
				c.events <- StateChange{CompletedTurns: turn, NewState: Executing}
				if _, _, err := remote.Command(BrokerCommandResume); err != nil {
					log.Printf("[Controller] resume command failed: %v", err)
				}
			} else {
				paused = true
				log.Printf("Paused at turn %d", turn)
				c.events <- StateChange{CompletedTurns: turn, NewState: Paused}
				if _, _, err := remote.Command(BrokerCommandPause); err != nil {
					log.Printf("[Controller] pause command failed: %v", err)
				}
			}
		case 's':
			saveWorld(c, world, width, height, turn)
		case 'q':
			quitting = true
			snapshotWorld := func() {
				if snapshot, snapTurn, err := remote.Snapshot(); err == nil {
					world = snapshot
					turn = snapTurn
					currentTurn.Lock()
					currentTurn.value = turn
					currentTurn.Unlock()
					alive := collectAliveCells(world)
					currentAlive.Lock()
					currentAlive.value = len(alive)
					currentAlive.Unlock()
				}
			}
			snapshotWorld()
			if _, _, err := remote.Command(BrokerCommandDetach); err != nil {
				log.Printf("[Controller] detach command failed: %v", err)
			}
		case 'k':
			quitting = true
			shutdownRequested = true
			if _, _, err := remote.Command(BrokerCommandShutdown); err != nil {
				log.Printf("[Controller] shutdown command failed: %v", err)
			}
		}
	}

	waitKey := func() {
		if keyPresses == nil {
			time.Sleep(10 * time.Millisecond)
			return
		}
		key := <-keyPresses
		handleKey(key)
	}

	drainKeys := func() {
		if keyPresses == nil {
			return
		}
	Drain:
		for {
			select {
			case key := <-keyPresses:
				handleKey(key)
				if quitting || paused {
					return
				}
			default:
				break Drain
			}
		}
	}
	// Main execution loop
	for turn < turnLimit && !quitting {
		if paused {
			waitKey()
			continue
		}

		if keyPresses != nil {
			select {
			case key := <-keyPresses:
				handleKey(key)
				if quitting || paused {
					continue
				}
			default:
			}
		}

		cells, aliveCount, err := remote.Step()
		if err != nil {
			log.Fatalf("[Distributor] Remote step failed: %v", err)
		}

		flipped := applyRemoteDiff(world, cells)

		turn++
		currentTurn.Lock()
		currentTurn.value = turn
		currentTurn.Unlock()
		currentAlive.Lock()
		currentAlive.value = aliveCount
		currentAlive.Unlock()

		if len(flipped) > 0 {
			cells := make([]util.Cell, len(flipped))
			copy(cells, flipped)
			c.events <- CellsFlipped{CompletedTurns: turn, Cells: cells}
		}
		c.events <- TurnComplete{CompletedTurns: turn}

		drainKeys()
	}

	aliveCells := collectAliveCells(world)
	currentTurn.Lock()
	currentTurn.value = turn
	currentTurn.Unlock()
	currentAlive.Lock()
	currentAlive.value = len(aliveCells)
	currentAlive.Unlock()
	c.events <- FinalTurnComplete{CompletedTurns: turn, Alive: aliveCells}
	saveWorld(c, world, width, height, turn)

	if !quitting && !shutdownRequested {
		if _, _, err := remote.Command(BrokerCommandDetach); err != nil {
			log.Printf("[Controller] detach command failed: %v", err)
		}
	}

	close(tickerStop)
	tickerWG.Wait()

	if shutdownRequested {
		shutdownBroker(client)
	}

	c.ioCommand <- ioCheckIdle
	<-c.ioIdle

	c.events <- StateChange{CompletedTurns: turn, NewState: Quitting}
	close(c.events)
}

func saveWorld(c distributorChannels, world [][]byte, width, height, turn int) {
	filename := fmt.Sprintf("%vx%vx%v", width, height, turn)
	c.ioCommand <- ioOutput
	c.ioFilename <- filename
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			value := byte(0)
			if y < len(world) && x < len(world[y]) {
				value = world[y][x]
			}
			c.ioOutput <- value
		}
	}
	c.ioCommand <- ioCheckIdle
	<-c.ioIdle
	c.events <- ImageOutputComplete{CompletedTurns: turn, Filename: filename}
}

func shutdownBroker(client *rpc.Client) {
	var reply WorkerShutdownResponse
	if err := client.Call("Broker.Shutdown", &WorkerShutdownRequest{}, &reply); err != nil {
		log.Printf("[Distributor] Broker shutdown failed: %v", err)
	}
}

type remoteSession struct {
	client    *rpc.Client
	sessionID string
	width     int
	height    int
}

func startRemoteSession(client *rpc.Client, p Params, world [][]byte) (*remoteSession, *BrokerStartSessionResponse, error) {
	if p.Resume {
		var resumeResp BrokerStartSessionResponse
		resumeReq := BrokerStartSessionRequest{
			ForceResume: true,
		}
		if err := client.Call("Broker.StartSession", &resumeReq, &resumeResp); err == nil {
			log.Printf("[Distributor] Resumed session %s at turn %d", resumeResp.SessionID, resumeResp.CompletedTurns)
			return &remoteSession{
				client:    client,
				sessionID: resumeResp.SessionID,
				width:     resumeResp.Width,
				height:    resumeResp.Height,
			}, &resumeResp, nil
		} else {
			log.Printf("[Distributor] Resume unavailable (%v); starting new session", err)
		}
	}

	req := BrokerStartSessionRequest{
		Params: p,
		World:  cloneWorld(world),
	}
	var resp BrokerStartSessionResponse
	if err := client.Call("Broker.StartSession", &req, &resp); err != nil {
		return nil, nil, err
	}
	return &remoteSession{
		client:    client,
		sessionID: resp.SessionID,
		width:     resp.Width,
		height:    resp.Height,
	}, &resp, nil
}

func (r *remoteSession) Step() ([]CellStateChange, int, error) {
	var resp BrokerStepResponse
	if err := r.client.Call("Broker.Step", &BrokerStepRequest{SessionID: r.sessionID}, &resp); err != nil {
		return nil, 0, err
	}
	return resp.Cells, resp.AliveCount, nil
}

func (r *remoteSession) Snapshot() ([][]byte, int, error) {
	var resp BrokerSnapshotResponse
	if err := r.client.Call("Broker.Snapshot", &BrokerSnapshotRequest{SessionID: r.sessionID}, &resp); err != nil {
		return nil, 0, err
	}
	return resp.World, resp.CompletedTurns, nil
}

func (r *remoteSession) Command(cmd BrokerCommand) (State, int, error) {
	var resp BrokerCommandResponse
	if err := r.client.Call("Broker.Command", &BrokerCommandRequest{SessionID: r.sessionID, Command: cmd}, &resp); err != nil {
		return Executing, 0, err
	}
	return resp.State, resp.CompletedTurns, nil
}

func applyRemoteDiff(world [][]byte, cells []CellStateChange) []util.Cell {
	if world == nil {
		return nil
	}
	flipped := make([]util.Cell, 0, len(cells))
	height := len(world)
	width := 0
	if height > 0 {
		width = len(world[0])
	}
	for _, cell := range cells {
		if cell.Y < 0 || cell.Y >= height || cell.X < 0 || cell.X >= width {
			continue
		}
		prev := world[cell.Y][cell.X]
		if cell.Alive {
			world[cell.Y][cell.X] = 255
		} else {
			world[cell.Y][cell.X] = 0
		}
		if prev != world[cell.Y][cell.X] {
			flipped = append(flipped, util.Cell{X: cell.X, Y: cell.Y})
		}
	}
	return flipped
}
