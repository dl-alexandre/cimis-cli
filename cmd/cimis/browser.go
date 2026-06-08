package main

import (
	"fmt"
	"os/exec"
	"runtime"
)

var (
	lookPath            = exec.LookPath
	startBrowserCommand = func(cmd string, args ...string) error {
		return exec.Command(cmd, args...).Start()
	}
	runtimeGOOS = runtime.GOOS
)

type BrowserOpener interface {
	Open(url string) error
}

type systemBrowserOpener struct{}

func (s *systemBrowserOpener) Open(url string) error {
	var cmd string
	var args []string

	switch runtimeGOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start", url}
	case "darwin":
		cmd = "open"
		args = []string{url}
	default:
		cmd = "xdg-open"
		args = []string{url}
	}

	if _, err := lookPath(cmd); err != nil {
		return fmt.Errorf("no browser command found: %s not found. Please install a browser or manually visit: %s", cmd, url)
	}

	if err := startBrowserCommand(cmd, args...); err != nil {
		return fmt.Errorf("failed to open browser: %w", err)
	}
	return nil
}

var browserOpener BrowserOpener = &systemBrowserOpener{}

func openBrowser(url string) error {
	return browserOpener.Open(url)
}

func cmdRegister() {
	const registerURL = "https://cimis.water.ca.gov/Welcome.aspx"

	fmt.Println("Opening CIMIS registration page in your browser...")
	fmt.Printf("URL: %s\n", registerURL)

	if err := openBrowser(registerURL); err != nil {
		logFatalf("Failed to open browser: %v\nPlease manually visit: %s\n", err, registerURL)
	}

	fmt.Println("Browser opened successfully!")
	fmt.Println("\nAfter registering:")
	fmt.Println("1. Check your email for verification link")
	fmt.Println("2. Login to access your API key")
	fmt.Println("3. Use 'cimis login' command to open the login page")
}

func cmdLogin() {
	const loginURL = "https://cimis.water.ca.gov/Auth/Login.aspx"

	fmt.Println("Opening CIMIS login page in your browser...")
	fmt.Printf("URL: %s\n", loginURL)

	if err := openBrowser(loginURL); err != nil {
		logFatalf("Failed to open browser: %v\nPlease manually visit: %s\n", err, loginURL)
	}

	fmt.Println("Browser opened successfully!")
	fmt.Println("\nAfter logging in:")
	fmt.Println("1. Navigate to the API/Web Services section")
	fmt.Println("2. Find your API key (app key)")
	fmt.Println("3. Use it with: cimis <command> -app-key=YOUR_KEY")
	fmt.Println("4. Or set environment variable: export CIMIS_APP_KEY=YOUR_KEY")
}

func cmdAPI() {
	const apiURL = "https://et.water.ca.gov/"

	fmt.Println("Opening CIMIS API documentation page in your browser...")
	fmt.Printf("URL: %s\n", apiURL)

	if err := openBrowser(apiURL); err != nil {
		logFatalf("Failed to open browser: %v\nPlease manually visit: %s\n", err, apiURL)
	}

	fmt.Println("Browser opened successfully!")
	fmt.Println("\nThis page contains information about CIMIS API access.")
	fmt.Println("Note: Full API access requires login.")
}
