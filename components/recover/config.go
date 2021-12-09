package recover

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/iosmanthus/learner-recover/common"

	"github.com/pingcap/tiup/pkg/cluster/spec"
	"gopkg.in/yaml.v3"
)

type Config struct {
	ClusterVersion string
	Patch          string
	ClusterName    string
	User           string
	ExtraSSHOpts   []string
	Zones          [][]*spec.TiKVSpec
	Labels         []map[string]string
	NewTopology    struct {
		Path      string
		PDServers []*spec.PDSpec
	}
	NewPlacementRules string
	PDBootstrap       []string
	JoinTopology      []string
	RecoverInfoFile   *common.RecoverInfo
	TiKVCtl           struct {
		Src  string
		Dest string
	}
	PDRecoverPath string
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		ClusterVersion    string              `yaml:"cluster-version"`
		ExtraSSHOpts      string              `yaml:"extra-ssh-opts"`
		Patch             string              `yaml:"patch"`
		ClusterName       string              `yaml:"cluster-name"`
		OldTopology       string              `yaml:"old-topology"`
		NewTopology       string              `yaml:"new-topology"`
		JoinTopology      string              `yaml:"join-topology"`
		NewPlacementRules string              `yaml:"new-placement-rules"`
		PDBootstrap       []string            `yaml:"pd-ctl-commands"`
		RecoverInfoFile   string              `yaml:"recover-info-file"`
		ZoneLabels        []map[string]string `yaml:"zone-labels"`
		TiKVCtl           struct {
			Src  string `yaml:"src"`
			Dest string `yaml:"dest"`
		} `yaml:"tikv-ctl"`
		PDRecoverPath string `yaml:"pd-recover-path"`
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &_Config{}
	if err = yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	if len(c.ZoneLabels) == 0 {
		return nil, errors.New("empty zone labels")
	}

	topo, err := common.ParseTiUPTopology(c.OldTopology)
	if err != nil {
		return nil, err
	}

	data, err = ioutil.ReadFile(c.RecoverInfoFile)
	if err != nil {
		return nil, err
	}

	info := &common.RecoverInfo{}
	if err = json.Unmarshal(data, info); err != nil {
		return nil, err
	}

	var zones [][]*spec.TiKVSpec
	for _, labels := range c.ZoneLabels {
		var nodes []*spec.TiKVSpec
		for _, tikv := range topo.TiKVServers {
			serverLabels, err := tikv.Labels()
			if err != nil {
				return nil, err
			}
			if common.IsLabelsMatch(labels, serverLabels) {
				nodes = append(nodes, tikv)
			}
		}
		if len(nodes) == 0 {
			return nil, fmt.Errorf("no TiKV nodes in the cluster, please check the topology file, labels: %v", labels)
		}
		zones = append(zones, nodes)
	}

	if err := checkDuplicate(zones); err != nil {
		return nil, err
	}

	newTopo, err := common.ParseTiUPTopology(c.NewTopology)
	if err != nil {
		return nil, err
	}

	var sshArgs []string
	if c.ExtraSSHOpts != "" {
		sshArgs = strings.Split(c.ExtraSSHOpts, " ")
	}

	joinTopo, err := common.ParseTiUPTopology(c.JoinTopology)
	if err != nil {
		return nil, err
	}

	var joinFiles []string
	for _, labels := range c.ZoneLabels {
		var nodes []*spec.TiKVSpec
		for _, tikv := range joinTopo.TiKVServers {
			serverLabels, err := tikv.Labels()
			if err != nil {
				return nil, err
			}
			if common.IsLabelsMatch(labels, serverLabels) {
				nodes = append(nodes, tikv)
			}
		}
		if len(nodes) == 0 {
			return nil, fmt.Errorf("no TiKV nodes in the join topology, please check the topology file, labels: %v", labels)
		}

		spec := &spec.Specification{TiKVServers: nodes}
		data, err := yaml.Marshal(spec)
		if err != nil {
			return nil, err
		}
		zoneLabels := "join-"
		for k, v := range labels {
			zoneLabels += k + v
		}
		filename := zoneLabels + ".yaml"
		err = ioutil.WriteFile(filename, data, 0644)
		if err != nil {
			return nil, err
		}
		joinFiles = append(joinFiles, filename)
	}

	return &Config{
		ClusterVersion:    c.ClusterVersion,
		Patch:             c.Patch,
		ExtraSSHOpts:      sshArgs,
		NewPlacementRules: c.NewPlacementRules,
		ClusterName:       c.ClusterName,
		User:              topo.GlobalOptions.User,
		Zones:             zones,
		Labels:            c.ZoneLabels,
		NewTopology: struct {
			Path      string
			PDServers []*spec.PDSpec
		}{c.NewTopology, newTopo.PDServers},
		JoinTopology:    joinFiles,
		RecoverInfoFile: info,
		PDBootstrap:     c.PDBootstrap,
		TiKVCtl: struct {
			Src  string
			Dest string
		}{
			Src:  c.TiKVCtl.Src,
			Dest: c.TiKVCtl.Dest,
		},
		PDRecoverPath: c.PDRecoverPath,
	}, nil
}

func checkDuplicate(zones [][]*spec.TiKVSpec) error {
	check := make(map[string]bool)
	for _, zone := range zones {
		for _, node := range zone {
			if _, ok := check[node.Host]; ok {
				return fmt.Errorf("duplicate host: %s", node.Host)
			}
			check[node.Host] = true
		}
	}
	return nil
}
