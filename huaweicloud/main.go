package main

import (
	"flag"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/logrusutil"
	"github.com/opensourceways/community-robot-lib/mq"
	liboptions "github.com/opensourceways/community-robot-lib/options"
	"github.com/opensourceways/xihe-training-center/controller"
	"github.com/opensourceways/xihe-training-center/docs"
	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/huaweicloud/syncjobstatusimpl"
	"github.com/opensourceways/xihe-training-center/huaweicloud/syncrepoimpl"
	"github.com/opensourceways/xihe-training-center/huaweicloud/trainingimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/mysql"
	"github.com/opensourceways/xihe-training-center/infrastructure/platformimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/synclockimpl"
	"github.com/opensourceways/xihe-training-center/server"
	"github.com/sirupsen/logrus"
	"os"
)

type options struct {
	service liboptions.ServiceOptions
}

func (o *options) Validate() error {
	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.Parse(args)
	return o
}

func main() {
	logrusutil.ComponentInit("xihe-training-center")
	log := logrus.NewEntry(logrus.StandardLogger())

	o := gatherOptions(
		flag.NewFlagSet(os.Args[0], flag.ExitOnError),
		os.Args[1:]...,
	)
	if err := o.Validate(); err != nil {
		logrus.Fatalf("Invalid options, err:%s", err.Error())
	}

	// cfg
	cfg, err := loadConfig(o.service.ConfigFile)
	if err != nil {
		logrus.Fatalf("load config, err:%s", err.Error())
	}

	// domain
	domain.Init(&cfg.Domain)

	// controller
	controller.Init(log)

	// sync repo
	sync, err := syncrepoimpl.NewSyncRepo(&cfg.Sync)
	if err != nil {
		logrus.Fatalf("init sync repo failed, err:%s", err.Error())
	}

	// gitlab
	p, err := platformimpl.NewPlatform(&cfg.Gitlab)
	if err != nil {
		logrus.Fatalf("init gitlab failed, err:%s", err.Error())
	}

	// sync lock
	if err := mysql.Init(&cfg.Mysql); err != nil {
		logrus.Fatalf("init gitlab failed, err:%s", err.Error())
	}

	lock := synclockimpl.NewRepoSyncLock(mysql.NewSyncLockMapper())

	// run
	ts, err := trainingimpl.NewTraining(&cfg.Training)
	if err != nil {
		logrus.Fatalf("new training center, err:%s", err.Error())
	}

	if err := initMQ(cfg.MQ, log); err != nil {
		log.Fatalf("initialize mq failed, err:%v", err)
	}

	st, err := syncjobstatusimpl.NewSyncJobStatus(&cfg.Sync, &cfg.Training, &cfg.JobStatus, log)
	if err != nil {
		logrus.Fatalf("new sync job status, err:%s", err.Error())
	}

	go syncjobstatusimpl.Process(st)

	server.StartWebServer(docs.SwaggerInfo, &server.Service{
		Port:     o.service.Port,
		Timeout:  o.service.GracePeriod,
		Sync:     sync,
		Lock:     lock,
		Log:      log,
		Platform: p,
		Training: ts,
	})
}

func initMQ(cfg mq.MQConfig, log *logrus.Entry) error {
	err := kafka.Init(
		mq.Addresses(cfg.Addresses...),
		mq.Log(log),
	)
	if err != nil {
		return err
	}

	return kafka.Connect()
}
