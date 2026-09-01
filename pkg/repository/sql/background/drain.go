package background

import (
	"runtime"
	"time"
)

type DrainConfig struct {
	BatchSize   int
	MaxBatches  int
	MaxDuration time.Duration
}

func normalizeDrainConfig(config DrainConfig) DrainConfig {
	if config.BatchSize <= 0 {
		config.BatchSize = defaultBackgroundBatchSize
	}
	if config.MaxBatches <= 0 {
		config.MaxBatches = defaultBackgroundMaxDrainBatches
	}
	if config.MaxDuration <= 0 {
		config.MaxDuration = defaultBackgroundCycleDuration
	}
	return config
}

func (s *SchedulerService) SetDrainConfig(config DrainConfig) {
	config = normalizeDrainConfig(config)
	s.batchSize = config.BatchSize
	s.maxDrainBatches = config.MaxBatches
	s.maxCycleDuration = config.MaxDuration
}

func (c *CronProcessorService) SetDrainConfig(config DrainConfig) {
	config = normalizeDrainConfig(config)
	c.batchSize = config.BatchSize
	c.maxDrainBatches = config.MaxBatches
	c.maxCycleDuration = config.MaxDuration
}

func (c *CalendarService) SetDrainConfig(config DrainConfig) {
	config = normalizeDrainConfig(config)
	c.batchSize = config.BatchSize
	c.maxDrainBatches = config.MaxBatches
	c.maxCycleDuration = config.MaxDuration
}

func (s *CleanupService) SetDrainConfig(config DrainConfig) {
	config = normalizeDrainConfig(config)
	s.batchSize = config.BatchSize
	s.maxDrainBatches = config.MaxBatches
	s.maxCycleDuration = config.MaxDuration
}

func yieldBetweenBatches(supportsSkipLocked bool) {
	if !supportsSkipLocked {
		runtime.Gosched()
	}
}
