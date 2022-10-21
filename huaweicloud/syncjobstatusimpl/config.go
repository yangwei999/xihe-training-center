package syncjobstatusimpl

type Config struct {
	SyncInterval int `json:"sync_interval"`
}

func (cfg *Config) SetDefault() {
	cfg.SyncInterval = 30
}
