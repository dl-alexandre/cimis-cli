package main

import (
	"testing"
)

func TestParseCacheSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		// Gigabytes
		{"1GB", 1 * 1024 * 1024 * 1024},
		{"1.5GB", int64(1.5 * 1024 * 1024 * 1024)},
		{"2gb", 2 * 1024 * 1024 * 1024},     // case insensitive
		{" 1GB ", 1 * 1024 * 1024 * 1024},   // whitespace

		// Megabytes
		{"100MB", 100 * 1024 * 1024},
		{"512MB", 512 * 1024 * 1024},
		{"1.5MB", int64(1.5 * 1024 * 1024)},

		// Kilobytes
		{"512KB", 512 * 1024},
		{"1024KB", 1024 * 1024},

		// Plain bytes
		{"1048576", 1048576},
		{"0", 0},

		// Invalid
		{"", 0},
		{"abc", 0},
		{"MB", 0},        // no number
		{"10XB", 0},      // unknown suffix
		{"-1MB", -1 * 1024 * 1024}, // negative values pass through ParseFloat
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := parseCacheSize(tt.input)
			if got != tt.want {
				t.Errorf("parseCacheSize(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseCacheSizeNegative(t *testing.T) {
	// Negative values: ParseFloat succeeds but result may be unexpected
	// This documents current behavior
	got := parseCacheSize("-1MB")
	if got >= 0 {
		// If it returns 0, that's fine (means parsing failed)
		// If negative, that's the current behavior
		t.Logf("parseCacheSize(\"-1MB\") = %d (documenting current behavior)", got)
	}
}

func TestParseStationList(t *testing.T) {
	tests := []struct {
		input   string
		want    []int
		wantErr bool
	}{
		// Single station
		{"2", []int{2}, false},
		{"100", []int{100}, false},

		// CSV list
		{"2,5,10", []int{2, 5, 10}, false},
		{" 2 , 5 , 10 ", []int{2, 5, 10}, false}, // with spaces

		// Range
		{"1-5", []int{1, 2, 3, 4, 5}, false},

		// Mixed
		{"2,5-7,10", []int{2, 5, 6, 7, 10}, false},

		// Errors
		{"abc", nil, true},
		{"1-2-3", nil, true},   // invalid range
		{"1-abc", nil, true},   // invalid range end
		{"abc-5", nil, true},   // invalid range start
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseStationList(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseStationList(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("parseStationList(%q) = %v, want %v", tt.input, got, tt.want)
					return
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("parseStationList(%q)[%d] = %d, want %d", tt.input, i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestSortStations(t *testing.T) {
	tests := []struct {
		name  string
		input []int
		want  []int
	}{
		{"already sorted", []int{1, 2, 3}, []int{1, 2, 3}},
		{"reverse", []int{3, 2, 1}, []int{1, 2, 3}},
		{"unsorted", []int{5, 1, 3, 2, 4}, []int{1, 2, 3, 4, 5}},
		{"single", []int{1}, []int{1}},
		{"empty", []int{}, []int{}},
		{"duplicates", []int{3, 1, 3, 2}, []int{1, 2, 3, 3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := make([]int, len(tt.input))
			copy(input, tt.input)
			sortStations(input)

			if len(input) != len(tt.want) {
				t.Errorf("sortStations(%v) = %v, want %v", tt.input, input, tt.want)
				return
			}
			for i := range input {
				if input[i] != tt.want[i] {
					t.Errorf("sortStations(%v)[%d] = %d, want %d", tt.input, i, input[i], tt.want[i])
				}
			}
		})
	}
}
