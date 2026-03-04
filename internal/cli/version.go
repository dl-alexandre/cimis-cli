package cli

// Build-time variables (set by GoReleaser or build flags)
var (
	// Version is the current version of the CLI
	Version = "dev"

	// BinaryName is the name of the binary
	BinaryName = "cimis"

	// GitHubRepo is the GitHub repository name
	GitHubRepo = "cimis-cli"

	// GitCommit is the git commit hash
	GitCommit = "unknown"

	// BuildTime is the build timestamp
	BuildTime = "unknown"
)
