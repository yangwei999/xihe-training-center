package watchimpl

import (
	"errors"
	"sync"
	"time"

	"github.com/opensourceways/xihe-grpc-protocol/grpc/client"
	pt "github.com/opensourceways/xihe-grpc-protocol/grpc/training"
	"github.com/sirupsen/logrus"

	"github.com/opensourceways/xihe-training-center/domain/training"
	"github.com/opensourceways/xihe-training-center/domain/watch"
)

type trainingData = pt.TrainingInfo

func NewWatcher(
	cfg *Config, ts training.Training,
	log *logrus.Entry,
) (*Watcher, error) {
	cli, err := client.NewTrainingClient(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	return &Watcher{
		log:         log,
		cli:         cli,
		ts:          ts,
		timeout:     cfg.Timeout,
		interval:    time.Duration(cfg.Interval) * time.Second,
		stop:        make(chan struct{}),
		stopped:     make(chan struct{}),
		trainings:   make(chan trainingInfo, cfg.MaxWatchNum+1),
		maxWatchNum: cfg.MaxWatchNum,
	}, nil
}

type trainingInfo struct {
	watch.TrainingInfo

	result trainingData

	done       bool
	success    bool
	logDone    bool
	aimDone    bool
	outputDone bool
}

func (t *trainingInfo) toIndex() pt.TrainingIndex {
	return pt.TrainingIndex{
		Id:        t.TrainingId,
		User:      t.User.Account(),
		ProjectId: t.ProjectId,
	}
}

func (t *trainingInfo) isDone() bool {
	done := t.done && t.logDone

	if done && t.success {
		done = t.outputDone && t.aimDone
	}

	return done
}

// Watcher
type Watcher struct {
	log *logrus.Entry
	cli *client.TrainingClient
	ts  training.Training

	timeout  int
	interval time.Duration

	stop      chan struct{}
	stopped   chan struct{}
	trainings chan trainingInfo

	lock        sync.RWMutex
	currentNum  int
	maxWatchNum int
}

func (w *Watcher) ApplyWatch(f func(*watch.TrainingInfo) error) (err error) {
	if !w.increase() {
		return errors.New("exceed max watch num")
	}

	info := new(watch.TrainingInfo)

	if err = f(info); err != nil {
		w.decrease()
	} else {
		w.addTraining(info)
	}

	return
}

func (w *Watcher) addTraining(t *watch.TrainingInfo) {
	info := trainingInfo{TrainingInfo: *t}
	if t.AimDir == "" {
		info.aimDone = true
	}

	if t.OutputDir == "" {
		info.outputDone = true
	}

	w.trainings <- info
}

func (w *Watcher) increase() (b bool) {
	w.lock.Lock()
	if w.currentNum+1 <= w.maxWatchNum {
		w.currentNum++
		b = true
	}
	w.lock.Unlock()

	return
}

func (w *Watcher) decrease() {
	w.lock.Lock()
	w.currentNum--
	w.lock.Unlock()
}

func (w *Watcher) Run() {
	start := time.Now()

	// add the tag
	w.trainings <- trainingInfo{}

	for {
		select {
		case info := <-w.trainings:
			// use =="" stands for the case that the loop is done
			if info.User == nil {
				w.log.Debug("finish a loop")

				t := start.Add(w.interval)

				if n := time.Now(); t.After(n) {
					time.Sleep(t.Sub(n))
				}

				w.trainings <- trainingInfo{}

				start = time.Now()

			} else {
				changed := w.check(&info)
				w.log.Debugf("check training %s/%s", info.TrainingId, info.JobId)

				if info.isDone() {
					index := info.toIndex()

					if err := w.cli.SetTrainingInfo(&index, &info.result); err == nil {
						w.decrease()
					} else {
						w.log.Errorf("set training info failed, err:%s", err.Error())
						w.trainings <- info
					}

				} else {
					if changed {
						index := info.toIndex()
						if err := w.cli.SetTrainingInfo(&index, &info.result); err != nil {
							w.log.Errorf("set training info failed, err:%s", err.Error())
						}
					}

					w.trainings <- info
				}
			}

		case <-w.stop:
			close(w.stopped)

			return
		}
	}
}

func (w *Watcher) Exit() {
	close(w.stop)

	<-w.stopped

	w.cli.Disconnect()
}

func (w *Watcher) check(info *trainingInfo) (changed bool) {
	result := &info.result

	if !info.done {
		detail, err := w.ts.GetDetail(info.JobId)
		if err != nil {
			return
		}

		if detail.Duration != result.Duration {
			result.Duration = detail.Duration
			changed = true
		}

		if s := detail.Status.TrainingStatus(); s != result.Status {
			result.Status = s
			changed = true
		}

		if !detail.Status.IsDone() {
			if detail.Duration < w.timeout {
				return
			}

			if err := w.ts.Terminate(info.JobId); err != nil {
				w.log.Errorf(
					"terminate the job(%s) failed, err:%s",
					info.JobId, err.Error(),
				)

				return
			}

			result.Status = "Timeout"
			changed = true
		} else {
			info.success = detail.Status.IsSuccess()
		}

		info.done = true
	}

	if !info.logDone {
		if v, err := w.ts.GetLogFilePath(info.LogDir); err != nil {
			w.log.Errorf("generate log failed, err:%s", err.Error())
		} else {
			result.LogPath = v
			info.logDone = true
			changed = true
		}
	}

	if !info.success {
		return
	}

	if !info.outputDone {
		if v, err := w.ts.GenOutput(info.OutputDir); err != nil {
			w.log.Errorf("generate output failed, err:%s", err.Error())
		} else {
			info.outputDone = true

			if v != "" {
				result.OutputZipPath = v
				changed = true
			}
		}
	}

	if !info.aimDone {
		if v, err := w.ts.GenAim(info.AimDir); err != nil {
			w.log.Errorf("generate aim failed, err:%s", err.Error())
		} else {
			info.aimDone = true

			if v != "" {
				result.AimZipPath = v
				changed = true
			}
		}
	}

	return
}
