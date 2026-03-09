package ports

// ExtractionParams holds parameters for starting a Fetcher extraction job.
type ExtractionParams struct {
	StartDate string
	EndDate   string
	Filters   map[string]any
}
