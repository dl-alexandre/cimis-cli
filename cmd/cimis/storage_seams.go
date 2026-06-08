package main

import (
	"github.com/dl-alexandre/cimis-tsdb/metadata"
	"github.com/dl-alexandre/cimis-tsdb/storage"
	"github.com/dl-alexandre/cimis-tsdb/types"
)

var (
	newChunkWriter    = storage.NewChunkWriter
	saveChunkMetadata = func(store *metadata.Store, chunk *types.ChunkInfo) error {
		return store.SaveChunk(chunk)
	}
	getChunksForYearRange = func(store *metadata.Store, stationID uint16, startYear, endYear int, dataType types.DataType) ([]types.ChunkInfo, error) {
		return store.GetChunksForYearRange(stationID, startYear, endYear, dataType)
	}
	getDatabaseStats = func(store *metadata.Store) (*metadata.DatabaseStats, error) {
		return store.GetDatabaseStats()
	}
)
