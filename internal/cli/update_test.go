package cli

import (
	"errors"
	"testing"

	"github.com/dl-alexandre/cli-tools/update"
)

type fakeUpdateChecker struct {
	info        *update.Info
	err         error
	checkForce  bool
	checkCalled bool
	autoCalled  bool
}

func (f *fakeUpdateChecker) Check(force bool) (*update.Info, error) {
	f.checkForce = force
	f.checkCalled = true
	return f.info, f.err
}

func (f *fakeUpdateChecker) AutoCheck() {
	f.autoCalled = true
}

func withFakeUpdateChecker(t *testing.T, fake *fakeUpdateChecker) {
	t.Helper()

	originalNew := newUpdateChecker
	originalDisplay := displayUpdate
	t.Cleanup(func() {
		newUpdateChecker = originalNew
		displayUpdate = originalDisplay
	})

	newUpdateChecker = func(config update.Config) updateChecker {
		if config.GitHubRepo != "dl-alexandre/cimis-cli" {
			t.Fatalf("GitHubRepo = %q, want dl-alexandre/cimis-cli", config.GitHubRepo)
		}
		if config.InstallCommand != "brew upgrade cimis" {
			t.Fatalf("InstallCommand = %q, want brew upgrade cimis", config.InstallCommand)
		}
		return fake
	}
}

func TestCheckForUpdatesUsesCheckerAndDisplay(t *testing.T) {
	fake := &fakeUpdateChecker{
		info: &update.Info{CurrentVersion: "v1.0.0", LatestVersion: "v1.0.1", UpdateAvailable: true},
	}
	withFakeUpdateChecker(t, fake)

	var displayCalled bool
	displayUpdate = func(info *update.Info, binaryName string, format string) error {
		displayCalled = true
		if info != fake.info {
			t.Fatal("display received unexpected update info")
		}
		if format != "json" {
			t.Fatalf("format = %q, want json", format)
		}
		return nil
	}

	if err := CheckForUpdates(true, "json"); err != nil {
		t.Fatalf("CheckForUpdates() error = %v", err)
	}
	if !fake.checkCalled {
		t.Fatal("checker was not called")
	}
	if !fake.checkForce {
		t.Fatal("force flag was not forwarded")
	}
	if !displayCalled {
		t.Fatal("displayUpdate was not called")
	}
}

func TestCheckForUpdatesPropagatesErrors(t *testing.T) {
	fake := &fakeUpdateChecker{err: errors.New("network down")}
	withFakeUpdateChecker(t, fake)

	displayUpdate = func(info *update.Info, binaryName string, format string) error {
		t.Fatal("displayUpdate should not run when check fails")
		return nil
	}

	if err := CheckForUpdates(false, "table"); err == nil {
		t.Fatal("expected check error")
	}
}

func TestAutoUpdateCheckUsesChecker(t *testing.T) {
	fake := &fakeUpdateChecker{}
	withFakeUpdateChecker(t, fake)

	AutoUpdateCheck()
	if !fake.autoCalled {
		t.Fatal("AutoCheck was not called")
	}
}

func TestDefaultUpdateCheckerFactory(t *testing.T) {
	checker := newUpdateChecker(update.Config{
		CurrentVersion: "v0.0.0",
		BinaryName:     "cimis",
		GitHubRepo:     "dl-alexandre/cimis-cli",
		InstallCommand: "brew upgrade cimis",
	})
	if checker == nil {
		t.Fatal("newUpdateChecker returned nil")
	}
}
