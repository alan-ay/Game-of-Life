package gol

import (
	"sync"
	"sync/atomic"
	"time"
)

// Params provides the details of how to run the Game of Life and which image to load.
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// Run starts the processing of Game of Life. It initialises coordination structs and goroutines.
func Run(p Params, events chan Event, keyPresses <-chan rune) {
	eventQueue := NewEventQueue()
	ioCoord := newIOCoordinator()
	go startIo(p, ioCoord)

	var eventForward sync.WaitGroup
	if events != nil {
		eventForward.Add(1)
		go func() {
			defer eventForward.Done()
			for {
				event, ok := eventQueue.Wait()
				if !ok {
					break
				}
				events <- event
			}
			close(events)
		}()
	} else {
		eventForward.Add(1)
		go func() {
			defer eventForward.Done()
			for {
				if _, ok := eventQueue.Wait(); !ok {
					return
				}
			}
		}()
	}

	var keyQueue *KeyQueue
	var keyBridgeStop atomic.Bool
	if keyPresses != nil {
		keyQueue = NewKeyQueue()
		go func() {
			for {
				if keyBridgeStop.Load() {
					return
				}
				select {
				case key, ok := <-keyPresses:
					if !ok {
						keyQueue.Close()
						return
					}
					if !keyQueue.Push(key) {
						return
					}
				default:
					if keyBridgeStop.Load() {
						return
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}

	distributor(p, eventQueue, ioCoord, keyQueue)
	ioCoord.Close()
	if keyQueue != nil {
		keyBridgeStop.Store(true)
		keyQueue.Close()
	}
	eventForward.Wait()
}
