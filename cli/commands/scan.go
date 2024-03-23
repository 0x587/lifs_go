package commands

import "github.com/urfave/cli/v2"

func CommandScan() *cli.Command {
	return &cli.Command{
		Name:    "scan",
		Aliases: []string{"s"},
		Action: func(context *cli.Context) error {
			return nil
		},
	}
}
