package gol

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"uk.ac.bris.cs/gameoflife/util"
)

type ioState struct {
	params Params
	coord  *ioCoordinator
}

// ioCommand allows requesting behaviour from the io (pgm) goroutine.
type ioCommand uint8

// This is a way of creating enums in Go.
// It will evaluate to:
//
//	ioOutput 	= 0
//	ioInput 	= 1
//	ioCheckIdle = 2
const (
	ioOutput ioCommand = iota
	ioInput
	ioCheckIdle
)

// writePgmImage receives an array of bytes and writes it to a pgm file.
func (io *ioState) writePgmImage(filename string, world [][]byte) {
	_ = os.Mkdir("out", os.ModePerm)

	file, ioError := os.Create("out/" + filename + ".pgm")
	util.Check(ioError)
	defer file.Close()

	_, _ = file.WriteString("P5\n")
	//_, _ = file.WriteString("# PGM file writer by pnmmodules (https://github.com/owainkenwayucl/pnmmodules).\n")
	_, _ = file.WriteString(strconv.Itoa(io.params.ImageWidth))
	_, _ = file.WriteString(" ")
	_, _ = file.WriteString(strconv.Itoa(io.params.ImageHeight))
	_, _ = file.WriteString("\n")
	_, _ = file.WriteString(strconv.Itoa(255))
	_, _ = file.WriteString("\n")

	for y := 0; y < io.params.ImageHeight; y++ {
		for x := 0; x < io.params.ImageWidth; x++ {
			_, ioError = file.Write([]byte{world[y][x]})
			util.Check(ioError)
		}
	}

	ioError = file.Sync()
	util.Check(ioError)

	log.Printf("[IO] File %v.pgm output done", filename)
}

// readPgmImage opens a pgm file and sends its data as an array of bytes.
func (io *ioState) readPgmImage(filename string, world [][]byte) {
	data, ioError := os.ReadFile("images/" + filename + ".pgm")
	util.Check(ioError)

	fields := strings.Fields(string(data))

	if fields[0] != "P5" {
		panic(fmt.Sprintf("[IO] %v %v is not a pgm file", util.Red("ERROR"), filename))
	}

	width, _ := strconv.Atoi(fields[1])
	if width != io.params.ImageWidth {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm width", util.Red("ERROR")))
	}

	height, _ := strconv.Atoi(fields[2])
	if height != io.params.ImageHeight {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm height", util.Red("ERROR")))
	}

	maxval, _ := strconv.Atoi(fields[3])
	if maxval != 255 {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm maxval/bit depth", util.Red("ERROR")))
	}

	image := []byte(fields[4])

	idx := 0
	for y := 0; y < io.params.ImageHeight; y++ {
		for x := 0; x < io.params.ImageWidth; x++ {
			world[y][x] = image[idx]
			idx++
		}
	}

	log.Printf("[IO] File %v.pgm input done", filename)
}

type ioRequest struct {
	command  ioCommand
	filename string
	buffer   [][]byte
}

type ioCoordinator struct {
	mu      sync.Mutex
	cond    *sync.Cond
	request *ioRequest
	closing bool
	idle    bool
}

func newIOCoordinator() *ioCoordinator {
	c := &ioCoordinator{idle: true}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *ioCoordinator) submit(command ioCommand, filename string, buffer [][]byte) {
	c.mu.Lock()
	for c.request != nil && !c.closing {
		c.cond.Wait()
	}
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.request = &ioRequest{command: command, filename: filename, buffer: buffer}
	c.idle = false
	c.cond.Signal()
	for c.request != nil && !c.closing {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

func (c *ioCoordinator) waitForRequest() (*ioRequest, bool) {
	c.mu.Lock()
	for c.request == nil && !c.closing {
		c.cond.Wait()
	}
	if c.closing {
		c.mu.Unlock()
		return nil, false
	}
	req := c.request
	c.mu.Unlock()
	return req, true
}

func (c *ioCoordinator) finishRequest() {
	c.mu.Lock()
	c.request = nil
	c.idle = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

func (c *ioCoordinator) ReadImage(filename string, target [][]byte) {
	c.submit(ioInput, filename, target)
}

func (c *ioCoordinator) WriteImage(filename string, source [][]byte) {
	c.submit(ioOutput, filename, source)
}

func (c *ioCoordinator) WaitIdle() {
	c.mu.Lock()
	for !c.idle && !c.closing {
		c.cond.Wait()
	}
	c.mu.Unlock()
}

func (c *ioCoordinator) Close() {
	c.mu.Lock()
	if c.closing {
		c.mu.Unlock()
		return
	}
	c.closing = true
	c.cond.Broadcast()
	c.mu.Unlock()
}

// startIo should be the entrypoint of the io goroutine.
func startIo(p Params, coord *ioCoordinator) {
	io := ioState{
		params: p,
		coord:  coord,
	}

	for {
		req, ok := coord.waitForRequest()
		if !ok {
			return
		}
		switch req.command {
		case ioInput:
			io.readPgmImage(req.filename, req.buffer)
		case ioOutput:
			io.writePgmImage(req.filename, req.buffer)
		case ioCheckIdle:
			// handled implicitly by WaitIdle in the distributor
		}
		coord.finishRequest()
	}
}
