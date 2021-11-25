package recover

import (
	"fmt"
	"github.com/iosmanthus/learner-recover/common"
	"sort"
	"strings"
)

type Resolver struct {
	resolveMap map[string][]*common.RegionState
	buffer     []*common.RegionState
	conflicts  []*common.RegionState
}

func NewResolver() *Resolver {
	return &Resolver{
		resolveMap: make(map[string][]*common.RegionState),
	}
}

func (r *Resolver) Merge(b *common.RegionInfos) {
	for _, state := range b.StateMap {
		r.buffer = append(r.buffer, state)
	}
}

func (r *Resolver) prepareResolve() {
	sort.SliceStable(r.buffer, func(i, j int) bool {
		cmpKey := strings.Compare(r.buffer[i].LocalState.Region.StartKey, r.buffer[j].LocalState.Region.StartKey)
		if cmpKey != 0 {
			return cmpKey < 0
		}

		cmpVersion := r.buffer[i].LocalState.Region.RegionEpoch.Version - r.buffer[j].LocalState.Region.RegionEpoch.Version
		if cmpVersion != 0 {
			return cmpVersion > 0
		}

		return r.buffer[i].ApplyState.AppliedIndex > r.buffer[j].ApplyState.AppliedIndex
	})

	for i := range r.buffer {
		pending := r.buffer[i].LocalState.Region.StartKey
		if _, ok := r.resolveMap[pending]; ok {
			queue := r.resolveMap[pending]
			last := queue[len(queue)-1]
			if last.LocalState.Region.RegionEpoch == r.buffer[i].LocalState.Region.RegionEpoch {
				continue
			}
		}
		r.resolveMap[pending] = append(r.resolveMap[pending], r.buffer[i])
	}
}

func (r *Resolver) TryResolve() error {
	r.prepareResolve()
	resolved := r.resolve("")
	if len(resolved) == 0 {
		return fmt.Errorf("fail to resolve")
	}

	resolvedMap := make(map[string]bool)
	for _, region := range resolved {
		key := fmt.Sprintf("%v@%s", region.RegionId, region.Host)
		resolvedMap[key] = true
	}

	for _, region := range r.buffer {
		key := fmt.Sprintf("%v@%s", region.RegionId, region.Host)
		if _, ok := resolvedMap[key]; !ok {
			r.conflicts = append(r.conflicts, region)
		}
	}

	return nil
}

func (r *Resolver) resolve(startKey string) []*common.RegionState {
	for _, state := range r.resolveMap[startKey] {
		if state.LocalState.Region.EndKey == "" {
			return []*common.RegionState{state}
		}
		rest := r.resolve(state.LocalState.Region.EndKey)
		if rest != nil {
			return append([]*common.RegionState{state}, rest...)
		}
	}
	delete(r.resolveMap, startKey)
	return nil
}
