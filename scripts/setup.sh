#!/bin/bash
# Setup script for cimis-cli development
# This script sets up the _deps/cimis-tsdb symlink for local development

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPS_DIR="$SCRIPT_DIR/_deps"
CIMIS_TSDB_DIR="$DEPS_DIR/cimis-tsdb"

echo "Setting up cimis-cli development environment..."

# Check if _deps/cimis-tsdb already exists
if [ -e "$CIMIS_TSDB_DIR" ]; then
    echo "✓ _deps/cimis-tsdb already exists"
    
    # Check if it's a symlink or directory
    if [ -L "$CIMIS_TSDB_DIR" ]; then
        echo "  (symlink to: $(readlink "$CIMIS_TSDB_DIR"))"
    elif [ -d "$CIMIS_TSDB_DIR" ]; then
        echo "  (regular directory)"
    fi
else
    # Create _deps directory if it doesn't exist
    mkdir -p "$DEPS_DIR"
    
    # Check if cimis-tsdb exists in common locations
    POSSIBLE_LOCATIONS=(
        "$SCRIPT_DIR/../cimis-tsdb"
        "$HOME/src/cimis-tsdb"
        "$HOME/code/cimis-tsdb"
        "$HOME/projects/cimis-tsdb"
    )
    
    FOUND=false
    for loc in "${POSSIBLE_LOCATIONS[@]}"; do
        if [ -d "$loc" ] && [ -f "$loc/go.mod" ]; then
            echo "Found cimis-tsdb at: $loc"
            ln -s "$loc" "$CIMIS_TSDB_DIR"
            echo "✓ Created symlink: _deps/cimis-tsdb -> $loc"
            FOUND=true
            break
        fi
    done
    
    if [ "$FOUND" = false ]; then
        echo "⚠ Could not find cimis-tsdb repository"
        echo ""
        echo "Please clone cimis-tsdb and run this script again, or manually create a symlink:"
        echo "  git clone https://github.com/dl-alexandre/cimis-tsdb.git ../cimis-tsdb"
        echo "  ln -s \"\$(pwd)/../cimis-tsdb\" _deps/cimis-tsdb"
        echo ""
        echo "Or provide the path as an argument:"
        echo "  ./scripts/setup.sh /path/to/cimis-tsdb"
        exit 1
    fi
fi

# Verify the dependency works
echo ""
echo "Verifying setup..."
if [ -f "$CIMIS_TSDB_DIR/go.mod" ]; then
    echo "✓ cimis-tsdb go.mod found"
else
    echo "✗ cimis-tsdb go.mod not found - check the symlink/path"
    exit 1
fi

# Run go mod tidy to verify
cd "$SCRIPT_DIR"
if go mod tidy 2>&1 | grep -q "error"; then
    echo "✗ go mod tidy failed - check the dependency setup"
    exit 1
else
    echo "✓ go mod tidy succeeded"
fi

echo ""
echo "Setup complete! You can now build:"
echo "  make build"
