package main

import "github.com/dl-alexandre/cimis-cli/internal/api"

var (
	newAPIClient          = api.NewClient
	newOptimizedAPIClient = api.NewOptimizedClient
)
