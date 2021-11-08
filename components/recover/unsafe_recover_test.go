package recover

import (
	"testing"

	"github.com/alecthomas/assert"
	"github.com/iosmanthus/learner-recover/common"
)

func TestResolve(t *testing.T) {
	a := new(common.RegionState)
	a.RegionId = 2
	a.LocalState.Region.StartKey = "1"
	a.LocalState.Region.EndKey = "4"
	a.ApplyState.AppliedIndex = 1162621
	a.LocalState.Region.RegionEpoch.Version = 7
	A := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			a.RegionId: a,
		},
	}

	b := new(common.RegionState)
	b.RegionId = 3
	b.LocalState.Region.StartKey = "4"
	b.LocalState.Region.EndKey = "6"
	b.ApplyState.AppliedIndex = 51
	b.LocalState.Region.RegionEpoch.Version = 7

	B := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			b.RegionId: b,
		},
	}

	c := new(common.RegionState)
	c.RegionId = 4
	c.LocalState.Region.StartKey = "6"
	c.LocalState.Region.EndKey = "8"
	c.ApplyState.AppliedIndex = 51
	c.LocalState.Region.RegionEpoch.Version = 7

	C := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			c.RegionId: c,
		},
	}

	r := NewResolveConflicts()
	r.Merge(nil, A)
	r.Merge(nil, C)
	r.Merge(nil, B)
	assert.Nil(t, r.conflicts)

	d := new(common.RegionState)
	d.RegionId = 5
	d.LocalState.Region.StartKey = "5"
	d.LocalState.Region.EndKey = "7"
	d.ApplyState.AppliedIndex = 51
	d.LocalState.Region.RegionEpoch.Version = 6

	D := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			d.RegionId: d,
		},
	}

	r.Merge(nil, D)
	assert.NotNil(t, r.conflicts)
	assert.Equal(t, len(r.conflicts), 1)

	e := new(common.RegionState)
	e.RegionId = 6
	e.LocalState.Region.StartKey = "5"
	e.LocalState.Region.EndKey = "7"
	e.ApplyState.AppliedIndex = 51
	e.LocalState.Region.RegionEpoch.Version = 8

	E := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			e.RegionId: e,
		},
	}

	r.Merge(nil, E)
	assert.Equal(t, len(r.conflicts), 3)

	f := new(common.RegionState)
	f.RegionId = 7
	f.LocalState.Region.StartKey = ""
	f.LocalState.Region.EndKey = ""
	f.ApplyState.AppliedIndex = 52
	f.LocalState.Region.RegionEpoch.Version = 8

	F := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			f.RegionId: f,
		},
	}

	r.Merge(nil, F)
	assert.Equal(t, len(r.conflicts), 5)
}
