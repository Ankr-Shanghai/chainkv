package main

import (
	"context"
	"fmt"
	"os"

	"github.com/panjf2000/gnet/v2"
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

	app.Commands = []*cli.Command{
		versionCommand,
	}

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

	ctxHandler, gs := grace.New(context.Background())

	gs.Register(func() error {
		srv.Stop(ctxHandler)
		return nil
	})

	gs.RegisterService("main service", func(ctx context.Context) error {
		err = gnet.Run(srv, srv.addr, gnet.WithMulticore(true))
		if err != nil {
			srv.log.Error("main service exit", "err", err)
		}
		return nil
	})

	gs.Wait()

	return nil
}
