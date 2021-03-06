package common

import (
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

type RegionId uint64

type RegionInfos struct {
	StateMap map[RegionId]*RegionState
}

func NewRegionInfos() *RegionInfos {
	return &RegionInfos{StateMap: make(map[RegionId]*RegionState)}
}

type Result struct {
	*RegionInfos
	Error error
}

func (r *RegionInfos) UnmarshalJSON(data []byte) error {
	tmp := make(map[string]map[RegionId]*RegionState)

	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	if _, ok := tmp["region_infos"]; !ok {
		return fmt.Errorf("missing region_infos field")
	}

	r.StateMap = tmp["region_infos"]

	for id, state := range r.StateMap {
		if state.ApplyState.AppliedIndex == 0 {
			delete(r.StateMap, id)
		}
	}

	return nil
}

type RegionState struct {
	RegionId   RegionId `json:"region_id"`
	Host       string
	SSHPort    int
	DataDir    string
	ApplyState struct {
		AppliedIndex uint64    `json:"applied_index"`
		Timestamp    time.Time `json:"timestamp"`
	} `json:"raft_apply_state"`
	LocalState struct {
		Region struct {
			StartKey    string `json:"start_key"`
			EndKey      string `json:"end_key"`
			RegionEpoch struct {
				Version int `json:"version"`
			} `json:"region_epoch"`
		} `json:"region"`
	} `json:"region_local_state"`
}

type Aggregator interface {
	Get() *RegionInfos
	Merge(b *RegionInfos)
}

type Fetcher interface {
	Fetch(ctx context.Context) (*RegionInfos, error)
}

type Collector interface {
	Collect(ctx context.Context, fetchers []Fetcher, m Aggregator) (*RegionInfos, error)
}

type RegionCollector struct{}

func NewRegionCollector() Collector {
	return &RegionCollector{}
}

func (f *RegionCollector) Collect(ctx context.Context, fetchers []Fetcher, m Aggregator) (*RegionInfos, error) {
	ch := make(chan Result, len(fetchers))

	for _, fetcher := range fetchers {
		go func(fetcher Fetcher) {
			ctx, cancel := context.WithTimeout(ctx, time.Minute*1)
			defer cancel()
			info, err := fetcher.Fetch(ctx)
			if err != nil {
				log.Errorf("fail to fetch region info from: %v: %v", fetcher, err)
				ch <- Result{Error: err}
				return
			}
			ch <- Result{RegionInfos: info}
		}(fetcher)
	}

	for i := 0; i < len(fetchers); i++ {
		result := <-ch
		if err := result.Error; err != nil {
			continue
		}
		m.Merge(result.RegionInfos)
	}

	return m.Get(), nil
}
