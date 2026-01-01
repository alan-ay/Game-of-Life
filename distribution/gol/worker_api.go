package gol

import "uk.ac.bris.cs/gameoflife/util"

// WorkerProcessRequest carries the config and starting board.
type WorkerProcessRequest struct {
	Params Params
	World  [][]byte
}

// WorkerProcessResponse returns the final board summary.
type WorkerProcessResponse struct {
	CompletedTurns int
	World          [][]byte
	Alive          []util.Cell
}

// WorkerSliceRequest carries a horizontal slice of the world with halo regions.
type WorkerSliceRequest struct {
	Slice      [][]byte
	Width      int
	Height     int
	StartRow   int
	HaloTop    []byte
	HaloBottom []byte
}

// WorkerSliceResponse returns the computed slice (without halos).
type WorkerSliceResponse struct {
	Slice [][]byte
}

// CellStateChange describes a single cell update.
type CellStateChange struct {
	X     int
	Y     int
	Alive bool
}

// BrokerStartSessionRequest is sent by the controller to bootstrap a session.
type BrokerStartSessionRequest struct {
	Params      Params
	World       [][]byte
	ResumeID    string
	ForceResume bool
}

// BrokerStartSessionResponse returns the broker session information.
type BrokerStartSessionResponse struct {
	SessionID      string
	Width          int
	Height         int
	CompletedTurns int
	AliveCount     int
	World          [][]byte
}

// BrokerStepRequest instructs the broker to advance a session by one turn.
type BrokerStepRequest struct {
	SessionID string
}

// BrokerStepResponse reports the diff for a completed turn.
type BrokerStepResponse struct {
	SessionID      string
	CompletedTurns int
	AliveCount     int
	Cells          []CellStateChange
}

// BrokerSnapshotRequest asks the broker for the full world.
type BrokerSnapshotRequest struct {
	SessionID string
}

// BrokerSnapshotResponse returns the latest board.
type BrokerSnapshotResponse struct {
	SessionID      string
	CompletedTurns int
	World          [][]byte
}

// BrokerCommand enumerates controller commands.
type BrokerCommand string

const (
	BrokerCommandPause    BrokerCommand = "pause"
	BrokerCommandResume   BrokerCommand = "resume"
	BrokerCommandDetach   BrokerCommand = "detach"
	BrokerCommandShutdown BrokerCommand = "shutdown"
)

// BrokerCommandRequest instructs the broker to change session state.
type BrokerCommandRequest struct {
	SessionID string
	Command   BrokerCommand
}

// BrokerCommandResponse returns the updated session status.
type BrokerCommandResponse struct {
	SessionID      string
	CompletedTurns int
	AliveCount     int
	State          State
	Message        string
}

// WorkerInitSliceRequest carries slice assignment metadata.
type WorkerInitSliceRequest struct {
	SessionID      string
	StartRow       int
	Width          int
	Height         int
	Slice          [][]byte
	TopHalo        []byte
	BottomHalo     []byte
	TopNeighbor    string
	BottomNeighbor string
	DirectHalo     bool
	CompressSlice  bool
}

// WorkerInitSliceResponse acknowledges slice assignment.
type WorkerInitSliceResponse struct {
	Assigned bool
}

// WorkerStepRequest advances the stored slice by one turn.
type WorkerStepRequest struct {
	SessionID  string
	Turn       int
	TopHalo    []byte
	BottomHalo []byte
}

// WorkerStepResponse returns slice diff metadata.
type WorkerStepResponse struct {
	SessionID    string
	StartRow     int
	Height       int
	AliveCount   int
	Cells        []CellStateChange
	EncodedSlice []byte
	ReadyForNext bool
}

// HaloDirection differentiates top/bottom halo exchanges.
type HaloDirection int

const (
	HaloDirectionTop HaloDirection = iota
	HaloDirectionBottom
)

// HaloExchangeRequest is used for direct worker-to-worker halo exchange.
type HaloExchangeRequest struct {
	SessionID string
	Turn      int
	Direction HaloDirection
	Row       []byte
}

// HaloExchangeResponse acknowledges receipt of halo row.
type HaloExchangeResponse struct {
	Row []byte
}

// AliveStatusRequest asks for the latest turn info.
type AliveStatusRequest struct{}

// AliveStatusResponse reports recent progress.
type AliveStatusResponse struct {
	CompletedTurns int
	AliveCount     int
}

// WorkerStopRequest tells the worker to stop.
type WorkerStopRequest struct{}

// WorkerStopResponse acknowledges a stop.
type WorkerStopResponse struct{}

// WorkerShutdownRequest tells the worker to terminate.
type WorkerShutdownRequest struct{}

// WorkerShutdownResponse acknowledges shutdown.
type WorkerShutdownResponse struct{}
