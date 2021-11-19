package rpo

import (
	"errors"
	"github.com/pingcap/tiup/pkg/cluster/spec"
	"io/ioutil"
	"strings"
	"time"

	"github.com/iosmanthus/learner-recover/common"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Voters      []*spec.TiKVSpec
	Learners    []*spec.TiKVSpec
	TiKVCtlPath struct {
		Src  string
		Dest string
	}
	VoterBackoffDuration   time.Duration
	LearnerBackoffDuration time.Duration
	RemoteUser             string
	HistoryPath            string
	ExtraSSHOpts           []string
	Save                   string
	LastFor                time.Duration
}

func NewConfig(path string) (*Config, error) {
	type _Config struct {
		Topology               string            `yaml:"topology"`
		LearnerLabels          map[string]string `yaml:"learner-labels"`
		VoterBackoffDuration   string            `yaml:"voter-backoff-duration"`
		LearnerBackoffDuration string            `yaml:"learner-backoff-duration"`
		TikvCtlPath            struct {
			Src  string `yaml:"src"`
			Dest string `yaml:"dest"`
		} `yaml:"tikv-ctl"`
		HistoryPath  string `yaml:"history-path"`
		ExtraSSHOpts string `yaml:"extra-ssh-opts"`
		Save         string `yaml:"save"`
		LastFor      string `yaml:"last-for"`
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	c := &_Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, err
	}

	lastFor, err := time.ParseDuration(c.LastFor)
	if err != nil {
		return nil, err
	}

	var voterBackoffDuration time.Duration
	if c.VoterBackoffDuration == "" {
		voterBackoffDuration = time.Second * 1
	} else {
		voterBackoffDuration, err = time.ParseDuration(c.VoterBackoffDuration)
		if err != nil {
			return nil, err
		}
	}

	var learnerBackoffDuration time.Duration
	if c.LearnerBackoffDuration == "" {
		learnerBackoffDuration = time.Second * 1
	} else {
		learnerBackoffDuration, err = time.ParseDuration(c.LearnerBackoffDuration)
		if err != nil {
			return nil, err
		}
	}

	topo, err := common.ParseTiUPTopology(c.Topology)
	if err != nil {
		return nil, err
	}

	var (
		voters   []*spec.TiKVSpec
		learners []*spec.TiKVSpec
	)

	for _, node := range topo.TiKVServers {
		serverLabels, err := node.Labels()
		if err != nil {
			return nil, err
		}

		if common.IsLabelsMatch(c.LearnerLabels, serverLabels) {
			learners = append(learners, node)
		} else {
			voters = append(voters, node)
		}
	}

	if len(voters) == 0 {
		return nil, errors.New("no voters in the cluster, please check the topology file")
	}
	if len(learners) == 0 {
		return nil, errors.New("no learners in the cluster, please check the topology file")
	}

	var sshArgs []string
	if c.ExtraSSHOpts != "" {
		sshArgs = strings.Split(c.ExtraSSHOpts, " ")
	}

	return &Config{
		Voters:                 voters,
		Learners:               learners,
		LearnerBackoffDuration: learnerBackoffDuration,
		VoterBackoffDuration:   voterBackoffDuration,
		TiKVCtlPath: struct {
			Src  string
			Dest string
		}{
			Src:  c.TikvCtlPath.Src,
			Dest: c.TikvCtlPath.Dest,
		},
		RemoteUser:   topo.GlobalOptions.User,
		ExtraSSHOpts: sshArgs,
		HistoryPath:  c.HistoryPath,
		Save:         c.Save,
		LastFor:      lastFor,
	}, nil
}
