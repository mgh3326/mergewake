package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/mgh3326/mergewake/internal/server/store"
)

func runServerStore(ctx context.Context, args []string, out, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: server migrate|check (credentials from MERGEWAKE_MYSQL_DSN or MERGEWAKE_MYSQL_DSN_FILE)")
	}
	command := args[0]
	if command != "migrate" && command != "check" {
		return errors.New("server currently supports migrate and check only")
	}
	fs := flag.NewFlagSet("server "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected server argument")
	}
	s, err := store.Connect(ctx)
	if err != nil {
		return err
	}
	defer s.Close()
	if command == "migrate" {
		err = s.Migrate(ctx)
	} else {
		err = s.CheckSchema(ctx)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "server store "+command+": ok")
	return err
}
