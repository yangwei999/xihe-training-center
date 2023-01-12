package sdk

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/opensourceways/community-robot-lib/utils"

	"github.com/opensourceways/xihe-training-center/app"
	"github.com/opensourceways/xihe-training-center/controller"
)

type TrainingCreateOption = controller.TrainingCreateRequest
type ResourceRef = controller.ResourceRef
type DownloadURL = controller.TrainingResultResp
type KeyValue = controller.KeyValue
type Compute = controller.Compute
type Input = controller.Input

type JobInfo = app.JobInfoDTO

func NewTrainingCenter(endpoint string) TrainingCenter {
	s := strings.TrimSuffix(endpoint, "/")
	if p := "api/v1/training"; !strings.HasSuffix(s, p) {
		s += p
	}

	return TrainingCenter{
		endpoint: s,
		cli:      utils.NewHttpClient(3),
	}
}

type TrainingCenter struct {
	endpoint string
	cli      utils.HttpClient
}

func (t TrainingCenter) jobURL(jobId string) string {
	return fmt.Sprintf("%s/%s", t.endpoint, jobId)
}

func (t TrainingCenter) CreateTraining(opt *TrainingCreateOption) (
	dto JobInfo, err error,
) {
	payload, err := utils.JsonMarshal(&opt)
	if err != nil {
		return
	}

	req, err := http.NewRequest(http.MethodPost, t.endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return
	}

	v := new(app.JobInfoDTO)
	if err = t.forwardTo(req, v); err != nil {
		return
	}

	return *v, nil
}

func (t TrainingCenter) DeleteTraining(jobId string) error {
	req, err := http.NewRequest(http.MethodDelete, t.jobURL(jobId), nil)
	if err != nil {
		return err
	}

	return t.forwardTo(req, nil)
}

func (t TrainingCenter) TerminateTraining(jobId string) error {
	req, err := http.NewRequest(http.MethodPut, t.jobURL(jobId), nil)
	if err != nil {
		return err
	}

	return t.forwardTo(req, nil)
}

func (t TrainingCenter) GetLogDownloadURL(jobId string) (r DownloadURL, err error) {
	req, err := http.NewRequest(http.MethodGet, t.jobURL(jobId)+"/log", nil)
	if err != nil {
		return
	}

	err = t.forwardTo(req, &r)

	return
}

func (t TrainingCenter) GetResultDownloadURL(jobId, file string) (r DownloadURL, err error) {
	req, err := http.NewRequest(http.MethodGet, t.jobURL(jobId)+"/result/"+file, nil)
	if err != nil {
		return
	}

	err = t.forwardTo(req, &r)

	return
}

func (t TrainingCenter) forwardTo(req *http.Request, jsonResp interface{}) (err error) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "xihe-training-center")

	if jsonResp != nil {
		v := struct {
			Data interface{} `json:"data"`
		}{jsonResp}

		_, err = t.cli.ForwardTo(req, &v)
	} else {
		_, err = t.cli.ForwardTo(req, jsonResp)
	}

	return
}
