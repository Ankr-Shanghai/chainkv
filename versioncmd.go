package main

import (
	"fmt"
	"runtime"

	"github.com/urfave/cli/v2"
)

const (
	VersionMajor = 0 // Major version component of the current release
	VersionMinor = 1 // Minor version component of the current release
	VersionPatch = 0 // Patch version component of the current release
)

var (
	clientIdentifier = "chainkv"
	GitCommit        string
	GitDate          string
)

var versionCommand = &cli.Command{
	Action:    printVersion,
	Name:      "version",
	Usage:     "Print version numbers",
	ArgsUsage: " ",
	Description: `
The output of this command is supposed to be machine-readable.
`,
}

func printVersion(ctx *cli.Context) error {

	fmt.Println(clientIdentifier)
	fmt.Println("Version:", fmt.Sprintf("%d.%d.%d", VersionMajor, VersionMinor, VersionPatch))
	if GitCommit != "" {
		fmt.Println("Git Commit:", GitCommit)
	}
	if GitDate != "" {
		fmt.Println("Git Commit Date:", GitDate)
	}
	fmt.Println("Architecture:", runtime.GOARCH)
	fmt.Println("Go Version:", runtime.Version())
	fmt.Println("Operating System:", runtime.GOOS)
	return nil
}
