package watchimpl

type Config struct {
	// Interval specifies the interval of second between two loops
	// that check all trainings in a loop.
	Interval int `json:"interval"`

	// Timeout specifies the time that a training can live
	// The unit is second.
	Timeout int `json:"timeout"`

	// MaxWatchNum specifies the max num of training
	// which the training center can support
	MaxWatchNum int `json:"max_watch_num"`

	Endpoint string `json:"endpoint" required:"true"`
}

func (cfg *Config) SetDefault() {
	if cfg.Interval <= 0 {
		cfg.Interval = 10
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = 86400
	}

	if cfg.MaxWatchNum <= 0 {
		cfg.MaxWatchNum = 100
	}
}
