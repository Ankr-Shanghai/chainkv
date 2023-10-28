package main

import (
	"context"
	"fmt"
	"os"

	"github.com/sunvim/utils/grace"
	"github.com/urfave/cli/v2"
)

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
}

var (
	app = cli.NewApp()
)

func init() {
	app.Name = "chainkv"
	app.Usage = "chainkv is key-value store service"
	app.Copyright = "Copyright 2023-Now The chainkv Authors"
	app.Action = kvact
	app.Flags = []cli.Flag{
		SvcHost,
		SvcPort,
		DataPath,
	}

	setHelpTemplate(app)
}

func kvact(ctx *cli.Context) error {
	host := ctx.String(SvcHost.Name)
	port := ctx.String(SvcPort.Name)
	datadir := ctx.String(DataPath.Name)
	srv, err := NewServer(host, port, datadir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(-1)
	}
	_, gs := grace.New(context.Background())

	gs.Register(func() error {
		srv.Stop()
		return nil
	})

	gs.RegisterService("chainkv", func(c context.Context) error {
		srv.Start()
		return nil
	})

	gs.Wait()

	return nil
}

func setHelpTemplate(app *cli.App) {
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Usage}}
USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[global options]{{end}}{{if .Commands}} command [command options]{{end}} {{if .ArgsUsage}}{{.ArgsUsage}}{{else}}[arguments...]{{end}}
  {{if len .Authors}}
AUTHOR:
  {{range .Authors}}{{ . }}{{end}}
  {{end}}{{if .Commands}}
COMMANDS:
 {{range .Commands}}{{if not .HideHelp}}   {{join .Names ","}}{{ "\t"}}{{.Usage}}{{ "\n" }}{{end}}{{end}}{{end}}{{if .VisibleFlags}}
GLOBAL OPTIONS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
  {{.Copyright}}
  {{end}}{{if .Version}}
VERSION:
  {{.Version}}
{{end}}
 `
}
