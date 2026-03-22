package worker

import "time"

// ArchivalWorkerConfig holds runtime configuration for the archival worker.
type ArchivalWorkerConfig struct {
	// Interval is how often the archival cycle runs.
	Interval time.Duration

	// HotRetentionDays is the number of days before a partition is eligible for warm transition.
	HotRetentionDays int

	// WarmRetentionMonths is the number of months before a partition is eligible for cold archival.
	WarmRetentionMonths int

	// ColdRetentionMonths is the total months to retain archives in cold storage.
	ColdRetentionMonths int

	// BatchSize is the number of rows per SELECT batch during data export.
	BatchSize int

	// StorageBucket is the object storage bucket for archives.
	StorageBucket string

	// StoragePrefix is the key prefix within the bucket.
	StoragePrefix string

	// StorageClass is the object storage class for cold-tier archives (e.g., "GLACIER").
	StorageClass string

	// PartitionLookahead is the number of future monthly partitions to create proactively.
	PartitionLookahead int
}
