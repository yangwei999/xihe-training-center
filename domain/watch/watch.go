package watch

import "github.com/opensourceways/xihe-training-center/domain"

type TrainingInfo struct {
	User       domain.Account
	ProjectId  string
	TrainingId string

	domain.JobInfo
}

type WatchService interface {
	ApplyWatch(f func(*TrainingInfo) error) (err error)
}
