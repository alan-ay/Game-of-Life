package gol

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

type workerResult struct {
	alive int
	flips []util.Cell
}

type turnCoordinator struct {
	mu       sync.Mutex
	cond     *sync.Cond
	turn     int
	pending  int
	results  []workerResult
	src      [][]byte
	dest     [][]byte
	shutdown bool
}

func newTurnCoordinator() *turnCoordinator {
	tc := &turnCoordinator{}
	tc.cond = sync.NewCond(&tc.mu)
	return tc
}

func (tc *turnCoordinator) startTurn(src, dest [][]byte, workerCount int) {
	tc.mu.Lock()
	tc.turn++
	tc.src = src
	tc.dest = dest
	tc.pending = workerCount
	if cap(tc.results) < workerCount {
		tc.results = make([]workerResult, 0, workerCount)
	} else {
		tc.results = tc.results[:0]
	}
	tc.cond.Broadcast()
	tc.mu.Unlock()
}

func (tc *turnCoordinator) waitForTurn(lastTurn int) (src, dest [][]byte, turn int, ok bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for !tc.shutdown && tc.turn == lastTurn {
		tc.cond.Wait()
	}
	if tc.shutdown {
		return nil, nil, 0, false
	}
	return tc.src, tc.dest, tc.turn, true
}

func (tc *turnCoordinator) submitResult(result workerResult) {
	tc.mu.Lock()
	if tc.pending > 0 {
		tc.results = append(tc.results, result)
		tc.pending--
		if tc.pending == 0 {
			tc.cond.Broadcast()
		}
	}
	tc.mu.Unlock()
}

func (tc *turnCoordinator) waitForResults() []workerResult {
	tc.mu.Lock()
	for tc.pending > 0 && !tc.shutdown {
		tc.cond.Wait()
	}
	results := append([]workerResult(nil), tc.results...)
	tc.mu.Unlock()
	return results
}

func (tc *turnCoordinator) stop() {
	tc.mu.Lock()
	tc.shutdown = true
	tc.cond.Broadcast()
	tc.mu.Unlock()
}

func runChunk(width, height, startY, endY int, src, dest [][]byte) workerResult {
	if height == 0 || width == 0 || startY >= endY {
		return workerResult{}
	}
	aliveCount := 0
	flipped := make([]util.Cell, 0)
	for y := startY; y < endY; y++ {
		for x := 0; x < width; x++ {
			aliveNeighbours := 0
			for dy := -1; dy <= 1; dy++ {
				for dx := -1; dx <= 1; dx++ {
					if dx == 0 && dy == 0 {
						continue
					}
					ny := (y + dy + height) % height
					nx := (x + dx + width) % width
					if src[ny][nx] == 255 {
						aliveNeighbours++
					}
				}
			}

			cellAlive := src[y][x] == 255
			var newVal byte
			switch {
			case cellAlive && (aliveNeighbours == 2 || aliveNeighbours == 3):
				newVal = 255
			case !cellAlive && aliveNeighbours == 3:
				newVal = 255
			default:
				newVal = 0
			}
			dest[y][x] = newVal
			if newVal != src[y][x] {
				flipped = append(flipped, util.Cell{X: x, Y: y})
			}
			if newVal == 255 {
				aliveCount++
			}
		}
	}
	return workerResult{alive: aliveCount, flips: flipped}
}

func duplicateBoard(world [][]byte) [][]byte {
	copyWorld := make([][]byte, len(world))
	for i := range world {
		copyWorld[i] = append([]byte(nil), world[i]...)
	}
	return copyWorld
}

// distributor divides the work between workers and interacts with other goroutines.
func distributor(p Params, events *EventQueue, ioCoord *ioCoordinator, keyQueue *KeyQueue) {

	width, height := p.ImageWidth, p.ImageHeight

	world := make([][]byte, height)
	for i := range world {
		world[i] = make([]byte, width)
	}
	next := make([][]byte, height)
	for i := range next {
		next[i] = make([]byte, width)
	}

	ioCoord.ReadImage(fmt.Sprintf("%vx%v", width, height), world)

	workerCount := p.Threads
	if workerCount < 1 {
		workerCount = 1
	}
	if height > 0 && workerCount > height {
		workerCount = height
	}
	if height == 0 {
		workerCount = 0
	}

	rowChunks := make([][2]int, 0, workerCount)
	if height > 0 && workerCount > 0 {
		rowsPerWorker := height / workerCount
		extra := height % workerCount
		start := 0
		for i := 0; i < workerCount; i++ {
			rows := rowsPerWorker
			if i < extra {
				rows++
			}
			end := start + rows
			rowChunks = append(rowChunks, [2]int{start, end})
			start = end
		}
	}
	initialAliveCells := make([]util.Cell, 0)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == 255 {
				initialAliveCells = append(initialAliveCells, util.Cell{X: x, Y: y})
			}
		}
	}

	turn := 0
	if len(initialAliveCells) > 0 {
		cells := make([]util.Cell, len(initialAliveCells))
		copy(cells, initialAliveCells)
		events.Push(CellsFlipped{CompletedTurns: turn, Cells: cells})
	}
	events.Push(StateChange{turn, Executing})

	var currentTurn atomic.Int64
	var currentAlive atomic.Int64
	currentTurn.Store(int64(turn))
	currentAlive.Store(int64(len(initialAliveCells)))

	const tickerInterval = 2 * time.Second
	const tickerCheck = 50 * time.Millisecond
	var tickerStop atomic.Bool
	var tickerWG sync.WaitGroup
	tickerWG.Add(1)
	go func() {
		defer tickerWG.Done()
		for !tickerStop.Load() {
			deadline := time.Now().Add(tickerInterval)
			for {
				if tickerStop.Load() {
					return
				}
				remaining := time.Until(deadline)
				if remaining <= 0 {
					break
				}
				if remaining > tickerCheck {
					time.Sleep(tickerCheck)
				} else {
					time.Sleep(remaining)
				}
			}
			if tickerStop.Load() {
				return
			}
			aliveCount := int(currentAlive.Load())
			completed := int(currentTurn.Load())
			events.Push(AliveCellsCount{CompletedTurns: completed, CellsCount: aliveCount})
		}
	}()

	paused := false
	quitRequested := false

	turnCtrl := newTurnCoordinator()
	var workerWG sync.WaitGroup
	for _, chunk := range rowChunks {
		workerWG.Add(1)
		startY := chunk[0]
		endY := chunk[1]
		go func() {
			defer workerWG.Done()
			lastTurn := 0
			for {
				src, dest, turnID, ok := turnCtrl.waitForTurn(lastTurn)
				if !ok {
					return
				}
				result := runChunk(width, height, startY, endY, src, dest)
				turnCtrl.submitResult(result)
				lastTurn = turnID
			}
		}()
	}

	saveWorld := func(board [][]byte, completed int) {
		filename := fmt.Sprintf("%vx%vx%v", width, height, completed)
		snapshot := duplicateBoard(board)
		ioCoord.WriteImage(filename, snapshot)
		events.Push(ImageOutputComplete{CompletedTurns: completed, Filename: filename})
	}

	handleKey := func(key rune, board [][]byte, completed int) {
		switch key {
		case 'p':
			if paused {
				paused = false
				events.Push(StateChange{CompletedTurns: completed, NewState: Executing})
			} else {
				paused = true
				events.Push(StateChange{CompletedTurns: completed, NewState: Paused})
			}
		case 's':
			saveWorld(board, completed)
		case 'q':
			quitRequested = true
		}
	}

	for turn < p.Turns && !quitRequested {
		if paused {
			if keyQueue == nil {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			key, ok := keyQueue.Pop()
			if !ok {
				continue
			}
			handleKey(key, world, turn)
			continue
		}

		aliveThisTurn := 0
		flippedCells := make([]util.Cell, 0)
		if len(rowChunks) == 0 {
			result := runChunk(width, height, 0, height, world, next)
			aliveThisTurn = result.alive
			if len(result.flips) > 0 {
				flippedCells = append(flippedCells, result.flips...)
			}
		} else {
			turnCtrl.startTurn(world, next, len(rowChunks))
			results := turnCtrl.waitForResults()
			for _, result := range results {
				aliveThisTurn += result.alive
				if len(result.flips) > 0 {
					flippedCells = append(flippedCells, result.flips...)
				}
			}
		}

		world, next = next, world
		turn++
		currentTurn.Store(int64(turn))
		currentAlive.Store(int64(aliveThisTurn))

		if len(flippedCells) > 0 {
			cells := make([]util.Cell, len(flippedCells))
			copy(cells, flippedCells)
			events.Push(CellsFlipped{CompletedTurns: turn, Cells: cells})
		}
		events.Push(TurnComplete{CompletedTurns: turn})

		if keyQueue != nil {
			for {
				key, ok := keyQueue.TryPop()
				if !ok {
					break
				}
				handleKey(key, world, turn)
				if quitRequested {
					break
				}
			}
		}
	}

	if workerCount > 0 {
		turnCtrl.stop()
	}
	workerWG.Wait()

	alive := make([]util.Cell, 0)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			if world[y][x] == 255 {
				alive = append(alive, util.Cell{X: x, Y: y})
			}
		}
	}

	events.Push(FinalTurnComplete{CompletedTurns: turn, Alive: alive})
	saveWorld(world, turn)

	tickerStop.Store(true)
	tickerWG.Wait()

	ioCoord.WaitIdle()

	events.Push(StateChange{turn, Quitting})
	if keyQueue != nil {
		keyQueue.Close()
	}

	events.Close()
}
