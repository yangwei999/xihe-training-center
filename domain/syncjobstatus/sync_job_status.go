package syncjobstatus

import (
	"github.com/opensourceways/xihe-training-center/app"
	"github.com/opensourceways/xihe-training-center/domain"
)

type SJTInterface interface {
	Init() error
	GetInterval() int
	Recovery() func(app.JobInfoDTO)
	GetJobs() []app.JobInfoDTO
	GetJobStatus(string) domain.JobDetail
	SyncJobStatus(domain.JobDetail, app.JobInfoDTO)
}
