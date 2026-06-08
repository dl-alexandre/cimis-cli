package cli

import (
	"github.com/dl-alexandre/cli-tools/update"
	"github.com/dl-alexandre/cli-tools/version"
)

type updateChecker interface {
	Check(force bool) (*update.Info, error)
	AutoCheck()
}

var (
	newUpdateChecker = func(config update.Config) updateChecker {
		return update.New(config)
	}
	displayUpdate = update.DisplayUpdate
)

// UpdateCheckCmd wraps cli-tools update functionality
type UpdateCheckCmd struct {
	Force  bool   `help:"Force check, bypassing cache" flag:"force"`
	Format string `help:"Output format" enum:"table,json" default:"table"`
}

// CheckForUpdates performs a manual update check and displays results
// This maintains backward compatibility with the old API
func CheckForUpdates(force bool, format string) error {
	cmd := &UpdateCheckCmd{
		Force:  force,
		Format: format,
	}
	return cmd.Run()
}

// Run executes the update check
func (c *UpdateCheckCmd) Run() error {
	checker := newUpdateChecker(update.Config{
		CurrentVersion: version.Version,
		BinaryName:     version.BinaryName,
		GitHubRepo:     "dl-alexandre/cimis-cli",
		InstallCommand: "brew upgrade cimis",
	})

	info, err := checker.Check(c.Force)
	if err != nil {
		return err
	}

	return displayUpdate(info, version.BinaryName, c.Format)
}

// AutoUpdateCheck performs a background update check (for use at startup)
// It returns immediately and doesn't block
func AutoUpdateCheck() {
	checker := newUpdateChecker(update.Config{
		CurrentVersion: version.Version,
		BinaryName:     version.BinaryName,
		GitHubRepo:     "dl-alexandre/cimis-cli",
		InstallCommand: "brew upgrade cimis",
	})
	checker.AutoCheck()
}

// UpdateInfo is re-exported from cli-tools for backward compatibility
type UpdateInfo = update.Info
