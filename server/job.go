package main

// Job is the request/response payload exchanged over HTTP. Item is the key,
// and Value is stored against it; Found and Status are filled on responses.
type Job struct {
	UUID    uint64 `json:"uuid,omitempty"`    // remote tracking reference id (optional)
	Status  int    `json:"status,omitempty"`  // job request status
	Item    string `json:"item,omitempty"`    // the KEY
	Value   uint64 `json:"value,omitempty"`   // the VALUE
	Found   bool   `json:"found,omitempty"`   // item found confirmation
	Version int    `json:"version,omitempty"` // version of the dataset
}

// Jobz is the put/ingest/export payload exchange over HTTP.
type Jobz struct {
	Item  string `json:"item,omitempty"`  // key
	Value uint64 `json:"value,omitempty"` // value
}

// Change is one buffered patch operation streamed to POST /patch: an upsert (the
// default) or, when Op == "delete", a removal. Item is the key; Value is stored
// for upserts and ignored for deletes.
type Change struct {
	Item  string `json:"item"`
	Value uint64 `json:"value,omitempty"`
	Op    string `json:"op,omitempty"` // "delete" → remove; empty/other → upsert
}
