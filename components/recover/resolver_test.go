package recover

import (
	"encoding/json"
	"fmt"
	"github.com/alecthomas/assert"
	"github.com/davecgh/go-spew/spew"
	"github.com/iosmanthus/learner-recover/common"
	"io/ioutil"
	"sort"
	"testing"
)

func assertFullRange(t *testing.T, ranges []*common.RegionState) {
	assert.True(t, len(ranges) > 0)
	assert.Equal(t, ranges[0].LocalState.Region.StartKey, "")

	var (
		prevEndKey = ranges[0].LocalState.Region.EndKey
	)

	for i, r := range ranges[1:] {
		assert.Equal(t, r.LocalState.Region.StartKey, prevEndKey, "prev: %s, current: %s", spew.Sdump(ranges[i]), spew.Sdump(ranges[i+1]))
		prevEndKey = r.LocalState.Region.EndKey
	}

	assert.Equal(t, prevEndKey, "")
}

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
	resolved, err := resolver.TryResolve()
	assert.Nil(t, err)
	assertFullRange(t, resolved)
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
	resolved, err := resolver.TryResolve()
	assert.Nil(t, err)
	assertFullRange(t, resolved)
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
	_, err := resolver.TryResolve()
	assert.NotNil(t, err)
}

func TestFromFile(t *testing.T) {
	resolver := NewResolver()
	for i := 1; i <= 5; i++ {
		path := fmt.Sprintf("../../tests/%v.json", i)
		data, err := ioutil.ReadFile(path)
		assert.Nil(t, err)

		info := common.NewRegionInfos()
		err = json.Unmarshal(data, info)
		assert.Nil(t, err)

		for _, s := range info.StateMap {
			s.Host = fmt.Sprintf("%v", i)
		}

		resolver.Merge(info)
	}

	resolved, err := resolver.TryResolve()
	assert.Nil(t, err)
	assertFullRange(t, resolved)

	for _, conflict := range resolver.conflicts {
		for _, region := range resolved {
			startKey := region.LocalState.Region.StartKey
			endKey := region.LocalState.Region.EndKey
			if startKey <= conflict.LocalState.Region.StartKey && (endKey == "" || endKey >= conflict.LocalState.Region.EndKey) {
				t.Logf("removed region: %s is conflicted with:\n %s", spew.Sdump(conflict), spew.Sdump(region))
			}
		}
	}
}
