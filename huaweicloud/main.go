package main

import (
	"flag"
	"os"

	"github.com/opensourceways/community-robot-lib/logrusutil"
	liboptions "github.com/opensourceways/community-robot-lib/options"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/xihe-training-center/app"
	"github.com/opensourceways/xihe-training-center/controller"
	"github.com/opensourceways/xihe-training-center/docs"
	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/huaweicloud/trainingimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/mysql"
	"github.com/opensourceways/xihe-training-center/infrastructure/platformimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/synclockimpl"
	"github.com/opensourceways/xihe-training-center/infrastructure/watchimpl"
	"github.com/opensourceways/xihe-training-center/server"
)

type options struct {
	service     liboptions.ServiceOptions
	enableDebug bool
}

func (o *options) Validate() error {
	return o.service.Validate()
}

func gatherOptions(fs *flag.FlagSet, args ...string) options {
	var o options

	o.service.AddFlags(fs)

	fs.BoolVar(
		&o.enableDebug, "enable_debug", false,
		"whether to enable debug model.",
	)

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

	if o.enableDebug {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.Debug("debug enabled.")
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

	// training
	ts, err := trainingimpl.NewTraining(&cfg.Train)
	if err != nil {
		logrus.Fatalf("new training center, err:%s", err.Error())
	}

	// watch
	ws, err := watchimpl.NewWatcher(&cfg.Watch, ts, log)
	if err != nil {
		log.Errorf("new watch service failed, err:%s", err.Error())
	}

	service := app.NewTrainingService(ts, p, ws, log, lock)

	go ws.Run()

	defer ws.Exit()

	server.StartWebServer(docs.SwaggerInfo, &server.Service{
		Port:     o.service.Port,
		Timeout:  o.service.GracePeriod,
		Log:      log,
		Training: service,
	})
}
