package recover

import (
	"github.com/alecthomas/assert"
	"github.com/iosmanthus/learner-recover/common"
	"sort"
	"testing"
)

func TestBase(t *testing.T) {
	a := new(common.RegionState)
	a.RegionId = 2
	a.Host = "2"
	a.LocalState.Region.StartKey = ""
	a.LocalState.Region.EndKey = "4"
	a.ApplyState.AppliedIndex = 1162621
	a.LocalState.Region.RegionEpoch.Version = 7

	b := new(common.RegionState)
	b.RegionId = 3
	b.Host = "3"
	b.LocalState.Region.StartKey = "4"
	b.LocalState.Region.EndKey = "6"
	b.ApplyState.AppliedIndex = 51
	b.LocalState.Region.RegionEpoch.Version = 7

	c := new(common.RegionState)
	c.RegionId = 4
	c.Host = "4"
	c.LocalState.Region.StartKey = "6"
	c.LocalState.Region.EndKey = "8"
	c.ApplyState.AppliedIndex = 51
	c.LocalState.Region.RegionEpoch.Version = 7

	d := new(common.RegionState)
	d.RegionId = 5
	d.Host = "5"
	d.LocalState.Region.StartKey = "6"
	d.LocalState.Region.EndKey = ""
	d.ApplyState.AppliedIndex = 51
	d.LocalState.Region.RegionEpoch.Version = 8

	samples := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			a.RegionId: a,
			b.RegionId: b,
			c.RegionId: c,
			d.RegionId: d,
		},
	}

	resolver := NewResolver()
	resolver.Merge(samples)
	assert.Nil(t, resolver.TryResolve())
	assert.Equal(t, resolver.conflicts[0].RegionId, common.RegionId(4))
	assert.Equal(t, resolver.conflicts[0].Host, "4")
}

func Test1123Issue(t *testing.T) {
	a := new(common.RegionState)
	a.RegionId = 2
	a.Host = "1"
	a.LocalState.Region.StartKey = ""
	a.LocalState.Region.EndKey = "1"
	a.ApplyState.AppliedIndex = 1162621
	a.LocalState.Region.RegionEpoch.Version = 7

	b := new(common.RegionState)
	b.RegionId = 3
	b.Host = "1"
	b.LocalState.Region.StartKey = "1"
	b.LocalState.Region.EndKey = "6"
	b.ApplyState.AppliedIndex = 51
	b.LocalState.Region.RegionEpoch.Version = 18

	c := new(common.RegionState)
	c.Host = "3"
	c.RegionId = 4
	c.LocalState.Region.StartKey = "1"
	c.LocalState.Region.EndKey = "6"
	c.ApplyState.AppliedIndex = 50
	c.LocalState.Region.RegionEpoch.Version = 18

	d := new(common.RegionState)
	d.RegionId = 5
	d.Host = "2"
	d.LocalState.Region.StartKey = "4"
	d.LocalState.Region.EndKey = "6"
	d.ApplyState.AppliedIndex = 51
	d.LocalState.Region.RegionEpoch.Version = 19

	e := new(common.RegionState)
	e.Host = "1"
	e.RegionId = 6
	e.LocalState.Region.StartKey = "6"
	e.LocalState.Region.EndKey = ""
	e.ApplyState.AppliedIndex = 51
	e.LocalState.Region.RegionEpoch.Version = 17

	samples := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			a.RegionId: a,
			b.RegionId: b,
			c.RegionId: c,
			d.RegionId: d,
			e.RegionId: e,
		},
	}

	resolver := NewResolver()
	resolver.Merge(samples)
	assert.Nil(t, resolver.TryResolve())
	sort.SliceStable(resolver.conflicts, func(i, j int) bool {
		return resolver.conflicts[i].RegionId < resolver.conflicts[j].RegionId
	})
	assert.Equal(t, len(resolver.conflicts), 2)
	assert.Equal(t, resolver.conflicts[0].RegionId, common.RegionId(4))
	assert.Equal(t, resolver.conflicts[0].Host, "3")

	assert.Equal(t, resolver.conflicts[1].RegionId, common.RegionId(5))
	assert.Equal(t, resolver.conflicts[1].Host, "2")
}

func TestFail(t *testing.T) {
	a := new(common.RegionState)
	a.RegionId = 2
	a.Host = "1"
	a.LocalState.Region.StartKey = ""
	a.LocalState.Region.EndKey = "1"
	a.ApplyState.AppliedIndex = 1162621
	a.LocalState.Region.RegionEpoch.Version = 7

	b := new(common.RegionState)
	b.RegionId = 3
	b.Host = "1"
	b.LocalState.Region.StartKey = "1"
	b.LocalState.Region.EndKey = "6"
	b.ApplyState.AppliedIndex = 51
	b.LocalState.Region.RegionEpoch.Version = 18

	c := new(common.RegionState)
	c.Host = "3"
	c.RegionId = 4
	c.LocalState.Region.StartKey = "1"
	c.LocalState.Region.EndKey = "6"
	c.ApplyState.AppliedIndex = 50
	c.LocalState.Region.RegionEpoch.Version = 18

	samples := &common.RegionInfos{
		StateMap: map[common.RegionId]*common.RegionState{
			a.RegionId: a,
			b.RegionId: b,
			c.RegionId: c,
		},
	}
	resolver := NewResolver()
	resolver.Merge(samples)
	assert.NotNil(t, resolver.TryResolve())
}
