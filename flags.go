package main

import "github.com/urfave/cli/v2"

var (
	SvcHost = &cli.StringFlag{
		Name:    "host",
		Aliases: []string{"H"},
		Value:   "127.0.0.1",
		Usage:   "host of the service",
	}

	SvcPort = &cli.IntFlag{
		Name:    "port",
		Aliases: []string{"p"},
		Value:   4321,
		Usage:   "port of the service",
	}

	DataPath = &cli.StringFlag{
		Name:    "datadir",
		Aliases: []string{"d"},
		Value:   "chainkv",
		Usage:   "path to store data",
	}
)
