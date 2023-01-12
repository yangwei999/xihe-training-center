package main

import (
	"github.com/opensourceways/community-robot-lib/utils"

	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/huaweicloud/trainingimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/mysql"
	"github.com/opensourceways/xihe-training-center/infrastructure/platformimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/watchimpl"
)

type configSetDefault interface {
	SetDefault()
}

type configValidate interface {
	Validate() error
}

type configuration struct {
	Train  trainingimpl.Config `json:"train"     required:"true"`
	Watch  watchimpl.Config    `json:"watch"     required:"true"`
	Mysql  mysql.Config        `json:"mysql"     required:"true"`
	Gitlab platformimpl.Config `json:"gitlab"    required:"true"`
	Domain domain.Config       `json:"domain"`
}

func (cfg *configuration) configItems() []interface{} {
	return []interface{}{
		&cfg.Watch,
		&cfg.Mysql,
		&cfg.Gitlab,
		&cfg.Domain,
		&cfg.Train,
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
