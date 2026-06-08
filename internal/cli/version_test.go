package cli

import "testing"

func TestVersionMetadata(t *testing.T) {
	if BinaryName == "" {
		t.Fatal("BinaryName should not be empty")
	}
	if GitHubRepo != "cimis-cli" {
		t.Fatalf("GitHubRepo = %q, want cimis-cli", GitHubRepo)
	}
}
