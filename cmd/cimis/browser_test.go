package main

import (
	"errors"
	"os"
	"strings"
	"testing"
)

type mockBrowserOpener struct {
	urls []string
	err  error
}

func (m *mockBrowserOpener) Open(url string) error {
	m.urls = append(m.urls, url)
	return m.err
}

func withFatalPanic(t *testing.T) {
	t.Helper()

	originalFatal := logFatal
	originalFatalf := logFatalf
	t.Cleanup(func() {
		logFatal = originalFatal
		logFatalf = originalFatalf
	})

	logFatal = func(v ...interface{}) {
		panic("fatal")
	}
	logFatalf = func(format string, v ...interface{}) {
		panic("fatalf")
	}
}

func expectPanic(t *testing.T, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

func TestBrowserCommandsUseConfiguredOpener(t *testing.T) {
	original := browserOpener
	defer func() { browserOpener = original }()

	mock := &mockBrowserOpener{}
	browserOpener = mock

	tests := []struct {
		name       string
		run        func()
		wantURL    string
		wantOutput string
	}{
		{"register", cmdRegister, "https://cimis.water.ca.gov/Welcome.aspx", "After registering"},
		{"login", cmdLogin, "https://cimis.water.ca.gov/Auth/Login.aspx", "After logging in"},
		{"api", cmdAPI, "https://et.water.ca.gov/", "CIMIS API access"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(mock.urls)
			output := captureStdout(t, tt.run)

			if len(mock.urls) != before+1 {
				t.Fatalf("browser opener calls = %d, want %d", len(mock.urls), before+1)
			}
			if got := mock.urls[len(mock.urls)-1]; got != tt.wantURL {
				t.Fatalf("opened URL = %q, want %q", got, tt.wantURL)
			}
			if !strings.Contains(output, tt.wantOutput) {
				t.Fatalf("output missing %q:\n%s", tt.wantOutput, output)
			}
		})
	}
}

func TestBrowserCommandsFatalOnOpenError(t *testing.T) {
	withFatalPanic(t)

	original := browserOpener
	defer func() { browserOpener = original }()

	tests := []struct {
		name string
		run  func()
	}{
		{"register", cmdRegister},
		{"login", cmdLogin},
		{"api", cmdAPI},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			browserOpener = &mockBrowserOpener{err: errors.New("open failed")}
			expectPanic(t, func() {
				captureStdout(t, tt.run)
			})
		})
	}
}

func TestFatalIfErr(t *testing.T) {
	fatalIfErr(nil)

	withFatalPanic(t)
	expectPanic(t, func() {
		fatalIfErr(errors.New("boom"))
	})
}

func TestSystemBrowserOpenerOpen(t *testing.T) {
	originalLookPath := lookPath
	originalStart := startBrowserCommand
	originalGOOS := runtimeGOOS
	defer func() {
		lookPath = originalLookPath
		startBrowserCommand = originalStart
		runtimeGOOS = originalGOOS
	}()

	tests := []struct {
		goos    string
		wantCmd string
	}{
		{"darwin", "open"},
		{"windows", "cmd"},
		{"linux", "xdg-open"},
	}

	for _, tt := range tests {
		t.Run(tt.goos, func(t *testing.T) {
			runtimeGOOS = tt.goos
			var gotLookPath string
			var gotCmd string
			var gotArgs []string
			lookPath = func(file string) (string, error) {
				gotLookPath = file
				return "/usr/bin/" + file, nil
			}
			startBrowserCommand = func(cmd string, args ...string) error {
				gotCmd = cmd
				gotArgs = append([]string(nil), args...)
				return nil
			}

			err := (&systemBrowserOpener{}).Open("https://example.test")
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			if gotLookPath != tt.wantCmd {
				t.Fatalf("lookPath command = %q, want %q", gotLookPath, tt.wantCmd)
			}
			if gotCmd != tt.wantCmd {
				t.Fatalf("start command = %q, want %q", gotCmd, tt.wantCmd)
			}
			if len(gotArgs) == 0 || gotArgs[len(gotArgs)-1] != "https://example.test" {
				t.Fatalf("start args = %v, want URL as final arg", gotArgs)
			}
		})
	}
}

func TestSystemBrowserOpenerOpenErrors(t *testing.T) {
	originalLookPath := lookPath
	originalStart := startBrowserCommand
	defer func() {
		lookPath = originalLookPath
		startBrowserCommand = originalStart
	}()

	lookPath = func(file string) (string, error) {
		return "", errors.New("missing")
	}
	if err := (&systemBrowserOpener{}).Open("https://example.test"); err == nil {
		t.Fatal("expected missing browser command error")
	}

	lookPath = func(file string) (string, error) {
		return "/usr/bin/" + file, nil
	}
	startBrowserCommand = func(cmd string, args ...string) error {
		return errors.New("start failed")
	}
	if err := (&systemBrowserOpener{}).Open("https://example.test"); err == nil {
		t.Fatal("expected start error")
	}
}

func TestDefaultStartBrowserCommand(t *testing.T) {
	if err := startBrowserCommand(os.Args[0], "-test.run=TestBrowserStartHelperProcess"); err != nil {
		t.Fatalf("startBrowserCommand() error = %v", err)
	}
}

func TestBrowserStartHelperProcess(t *testing.T) {}

func TestRunDispatchBrowserCommands(t *testing.T) {
	withNoAutoUpdate(t)

	original := browserOpener
	defer func() { browserOpener = original }()

	mock := &mockBrowserOpener{}
	browserOpener = mock

	for _, command := range []string{"register", "login", "api-docs"} {
		if code := run([]string{"cimis", command}); code != 0 {
			t.Fatalf("%s exit code = %d, want 0", command, code)
		}
	}
	if len(mock.urls) != 3 {
		t.Fatalf("browser opener calls = %d, want 3", len(mock.urls))
	}
}
