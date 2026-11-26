package main

import (
	"fmt"

	cliver "github.com/dl-alexandre/cli-tools/version"
)

type VersionCmd struct{}

func (VersionCmd) Run() error {
	fmt.Printf("cimis %s (%s) built %s\n", cliver.Version, cliver.GitCommit, cliver.BuildTime)
	return nil
}

type CheckUpdatesCmd struct {
	Force bool `help:"Force update check" name:"force"`
	JSON  bool `help:"Output update check as JSON" name:"json"`
}

// The legacy CLI exposed --json as a boolean switch. Kong stores it as a
// boolean-style flag; Run maps it to the update library's format argument.
func (c *CheckUpdatesCmd) Run() error {
	format := "table"
	if c.JSON {
		format = "json"
	}
	return checkForUpdates(c.Force, format)
}

type InitCmd struct{}

func (InitCmd) Run(globals *Globals) error { return runInit(globals.DataDir) }

type StatsCmd struct{}

func (StatsCmd) Run(globals *Globals) error { return runStats(globals.DataDir) }

type VerifyCmd struct{}

func (VerifyCmd) Run(globals *Globals) error { return runVerify(globals.DataDir) }

type RegisterCmd struct{}

//nolint:unparam // Kong command Run methods must return an error value.
func (RegisterCmd) Run() error { cmdRegister(); return nil }

type LoginCmd struct{}

//nolint:unparam // Kong command Run methods must return an error value.
func (LoginCmd) Run() error { cmdLogin(); return nil }

type APIDocsCmd struct{}

//nolint:unparam // Kong command Run methods must return an error value.
func (APIDocsCmd) Run() error { cmdAPI(); return nil }
