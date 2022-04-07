package transfer

import (
	"context"
	"fmt"
	"github.com/iosmanthus/learner-recover/common"
	"strings"
)

type Action struct {
	Version  string
	PromAddr string
	PDAddr   string
	Rule     string
}

func (a Action) apply(ctx context.Context) error {
	output, err := common.TiUP(ctx,
		fmt.Sprintf("ctl:%s", a.Version), "pd",
		"-u", a.PDAddr,
		"config", "placement-rules", "rule-bundle", "set", "pd", "--in="+a.Rule)

	if err != nil {
		return err
	}

	if !strings.Contains(output, "\"Update group and rules successfully.\"") {
		return fmt.Errorf("fail to apply placement rules: %s", output)
	}

	return nil
}
