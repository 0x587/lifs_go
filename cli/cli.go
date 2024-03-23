package cli

import (
	"github.com/urfave/cli/v2"
	cs "lifs_go/cli/commands"
)

func NewApp() *cli.App {
	app := &cli.App{
		Name:     "lifs",
		HelpName: "lifs",
		Commands: []*cli.Command{
			cs.CommandScan()},
	}

	return app
}
