package main

import (
	"io"

	"github.com/alecthomas/kong"
)

func parseCommandOptions(name string, args []string, target any) error {
	parser, err := kong.New(target,
		kong.Name(name),
		kong.Writers(io.Discard, io.Discard),
		kong.Exit(func(int) {}),
	)
	if err != nil {
		return err
	}
	_, err = parser.Parse(normalizeLegacyFlags(args))
	return err
}
