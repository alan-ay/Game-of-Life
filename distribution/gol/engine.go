package gol

import (
	"sync"

	"uk.ac.bris.cs/gameoflife/util"
)

// TurnCallback runs after a turn; return false to stop.
type TurnCallback func(turn int, alive int) bool

// SimulationResult summarises a completed run.
type SimulationResult struct {
	CompletedTurns int
	World          [][]byte
	Alive          []util.Cell
}

// RunSimulation steps the board locally and reports into the callback.
func RunSimulation(p Params, initial [][]byte, callback TurnCallback) SimulationResult {
	current := cloneWorld(initial)
	height := len(current)
	width := 0
	if height > 0 {
		width = len(current[0])
	}
	workers := effectiveWorkerCount(p.Threads, height)

	next := make([][]byte, height)
	for y := 0; y < height; y++ {
		next[y] = make([]byte, width)
	}

	completedTurns := 0
	// Share the initial snapshot before any turns.
	if callback != nil {
		alive := countAlive(current)
		if !callback(0, alive) {
			finalWorld := cloneWorld(current)
			aliveCells := collectAliveCells(current)
			return SimulationResult{CompletedTurns: completedTurns, World: finalWorld, Alive: aliveCells}
		}
	}

	for turn := 0; turn < p.Turns; turn++ {
		if height == 0 || width == 0 {
			completedTurns = turn + 1
			if callback != nil {
				if !callback(completedTurns, 0) {
					break
				}
			}
			current, next = next, current
			continue
		}

		aliveThisTurn := evolve(current, next, width, height, workers)

		current, next = next, current
		completedTurns = turn + 1
		if callback != nil {
			if !callback(completedTurns, aliveThisTurn) {
				break
			}
		}
	}

	finalWorld := cloneWorld(current)
	aliveCells := collectAliveCells(current)
	return SimulationResult{CompletedTurns: completedTurns, World: finalWorld, Alive: aliveCells}
}

// ComputeBoard runs the simulation to completion without reporting intermediate turns.
func ComputeBoard(p Params, initial [][]byte) SimulationResult {
	return RunSimulation(p, initial, nil)
}

func cloneWorld(src [][]byte) [][]byte {
	if src == nil {
		return nil
	}
	clone := make([][]byte, len(src))
	for i := range src {
		if src[i] == nil {
			continue
		}
		row := make([]byte, len(src[i]))
		copy(row, src[i])
		clone[i] = row
	}
	return clone
}

func collectAliveCells(world [][]byte) []util.Cell {
	cells := make([]util.Cell, 0)
	for y := range world {
		row := world[y]
		for x, cell := range row {
			if cell == 255 {
				cells = append(cells, util.Cell{X: x, Y: y})
			}
		}
	}
	return cells
}

func countAlive(world [][]byte) int {
	total := 0
	for y := range world {
		for _, cell := range world[y] {
			if cell == 255 {
				total++
			}
		}
	}
	return total
}

// CountAliveCells is an exported version of countAlive for use by broker.
func CountAliveCells(world [][]byte) int {
	return countAlive(world)
}

// CollectAliveCells is an exported version of collectAliveCells for use by broker.
func CollectAliveCells(world [][]byte) []util.Cell {
	return collectAliveCells(world)
}

func evolve(current, next [][]byte, width, height, workers int) int {
	if height == 0 || width == 0 {
		return 0
	}
	if workers <= 1 {
		return evolveRange(current, next, width, height, 0, height)
	}

	chunkSize := (height + workers - 1) / workers
	if chunkSize < 1 {
		chunkSize = 1
	}

	var wg sync.WaitGroup
	usedWorkers := 0
	partial := make([]int, workers)
	for start := 0; start < height && usedWorkers < workers; start += chunkSize {
		end := start + chunkSize
		if end > height {
			end = height
		}
		idx := usedWorkers
		usedWorkers++
		wg.Add(1)
		go func(i, s, e int) {
			defer wg.Done()
			partial[i] = evolveRange(current, next, width, height, s, e)
		}(idx, start, end)
	}
	wg.Wait()

	total := 0
	for i := 0; i < usedWorkers; i++ {
		total += partial[i]
	}
	return total
}

func evolveRange(current, next [][]byte, width, height, startY, endY int) int {
	alive := 0
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
					if current[ny][nx] == 255 {
						aliveNeighbours++
					}
				}
			}

			cellAlive := current[y][x] == 255
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
				alive++
			}
		}
	}
	return alive
}

func effectiveWorkerCount(requested, height int) int {
	if requested < 1 {
		return 1
	}
	if height <= 1 {
		return 1
	}
	if requested > height {
		return height
	}
	return requested
}
