package main

import (
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/opensourceways/community-robot-lib/utils"
	"github.com/opensourceways/xihe-training-center/huaweicloud/syncjobstatusimpl"

	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/huaweicloud/syncrepoimpl"
	"github.com/opensourceways/xihe-training-center/huaweicloud/trainingimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/mysql"
	"github.com/opensourceways/xihe-training-center/infrastructure/platformimpl"
)

type configSetDefault interface {
	SetDefault()
}

type configValidate interface {
	Validate() error
}

type configuration struct {
	Sync      syncrepoimpl.Config      `json:"sync"      required:"true"`
	Mysql     mysql.Config             `json:"mysql"     required:"true"`
	Gitlab    platformimpl.Config      `json:"gitlab"    required:"true"`
	Domain    domain.Config            `json:"domain"`
	Training  trainingimpl.Config      `json:"training"  required:"true"`
	MQ        mq.MQConfig              `json:"mq"`
	JobStatus syncjobstatusimpl.Config `json:"job_status"`
}

func (cfg *configuration) configItems() []interface{} {
	return []interface{}{
		&cfg.Sync,
		&cfg.Mysql,
		&cfg.Gitlab,
		&cfg.Domain,
		&cfg.Training,
		&cfg.JobStatus,
	}
}

func (cfg *configuration) validate() error {
	if _, err := utils.BuildRequestBody(cfg, ""); err != nil {
		return err
	}

	items := cfg.configItems()

	for _, i := range items {
		if v, ok := i.(configValidate); ok {
			if err := v.Validate(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (cfg *configuration) setDefault() {
	items := cfg.configItems()

	for _, i := range items {
		if v, ok := i.(configSetDefault); ok {
			v.SetDefault()
		}
	}
}

func loadConfig(path string) (*configuration, error) {
	v := new(configuration)

	if err := utils.LoadFromYaml(path, v); err != nil {
		return nil, err
	}

	v.setDefault()

	if err := v.validate(); err != nil {
		return nil, err
	}

	return v, nil
}
