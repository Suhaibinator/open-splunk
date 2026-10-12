package worker

// PipelineStage binds a persisted application stage ID to a queue lane. The
// declaration order defines the only permitted successful forward transitions.
type PipelineStage[S comparable, T any] struct {
	ID   S
	Lane Lane[T]
}

// LinearPipeline is an ordered set of independently draining stage lanes. It
// does not own persistence; consumers commit guarded transitions before waking
// the next lane. Failure and terminal result policy remain application callbacks.
type LinearPipeline[S comparable, T any] struct{ stages []PipelineStage[S, T] }

func NewLinearPipeline[S comparable, T any](stages ...PipelineStage[S, T]) *LinearPipeline[S, T] {
	ids := make(map[S]bool, len(stages))
	names := make(map[string]bool, len(stages))
	for _, stage := range stages {
		if ids[stage.ID] || names[stage.Lane.Name] || stage.Lane.Name == "" {
			panic("worker: duplicate stage ID or empty/duplicate lane name")
		}
		ids[stage.ID] = true
		names[stage.Lane.Name] = true
	}
	return &LinearPipeline[S, T]{stages: append([]PipelineStage[S, T](nil), stages...)}
}

func (p *LinearPipeline[S, T]) Lanes() []LaneRunner {
	lanes := make([]LaneRunner, 0, len(p.stages))
	for _, stage := range p.stages {
		lanes = append(lanes, NewLane(stage.Lane))
	}
	return lanes
}

// Next reports the successor, or false for a terminal/unknown stage.
func (p *LinearPipeline[S, T]) Next(id S) (S, bool) {
	for i, stage := range p.stages {
		if stage.ID == id && i+1 < len(p.stages) {
			return p.stages[i+1].ID, true
		}
	}
	var zero S
	return zero, false
}
