package syncjobstatusimpl

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/chnsz/golangsdk"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/opensourceways/community-robot-lib/kafka"
	"github.com/opensourceways/community-robot-lib/mq"
	"github.com/opensourceways/xihe-training-center/app"
	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/domain/syncjobstatus"
	"github.com/opensourceways/xihe-training-center/huaweicloud/client"
	"github.com/opensourceways/xihe-training-center/huaweicloud/modelarts"
	"github.com/opensourceways/xihe-training-center/huaweicloud/syncrepoimpl"
	"github.com/opensourceways/xihe-training-center/huaweicloud/trainingimpl"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"
)

const kafkaTopic = "training_job"
const urlExpire = 3600 * 24
const zipFileDir = "/tmp/syncjobstatus/"
const zipKeyPrefix = "jobStatusZip/"

type SyncJobStatus struct {
	obsClient *obs.ObsClient
	jobChan   chan app.JobInfoDTO
	log       *logrus.Entry
	cli       *golangsdk.ServiceClient
	bucket    string
	interval  int
}

type SyncData struct {
	Status domain.JobDetail `json:"status"`
	Log    []string         `json:"log"`
	Output string           `json:"output"`
}

var StatusContinue = map[string]domain.TrainingStatus{
	"Pending":  domain.TrainingStatusPending,
	"Running":  domain.TrainingStatusRunning,
	"Creating": domain.TrainingStatusCreating,
}

var StatusEnd = map[string]domain.TrainingStatus{
	"abnormal":    domain.TrainingStatusAbnormal,
	"Terminated":  domain.TrainingStatusTerminated,
	"Terminating": domain.TrainingStatusTerminating,
}

var StatusOK = map[string]domain.TrainingStatus{
	"Failed":    domain.TrainingStatusFailed,
	"Completed": domain.TrainingStatusCompleted,
}

func NewSyncJobStatus(
	scfg *syncrepoimpl.Config,
	tcfg *trainingimpl.Config,
	jcfg *Config,
	log *logrus.Entry) (*SyncJobStatus, error) {
	obsCli, err := obs.New(scfg.AccessKey, scfg.SecretKey, scfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("new obs client failed, err:%s", err.Error())
	}

	s := "modelarts"
	v := client.Config{
		AccessKey:  tcfg.AccessKey,
		SecretKey:  tcfg.SecretKey,
		TenantName: tcfg.ProjectName,
		TenantID:   tcfg.ProjectId,
		Region:     tcfg.Region,
		Endpoints: map[string]string{
			s: tcfg.Endpoint,
		},
		IdentityEndpoint: fmt.Sprintf("https://iam.%s.myhuaweicloud.com:443/v3", tcfg.Region),
	}
	cli, err := v.NewServiceClient(s, client.ServiceCatalog{
		Version: "v2",
	})

	return &SyncJobStatus{
		obsClient: obsCli,
		jobChan:   make(chan app.JobInfoDTO, 20),
		log:       log,
		bucket:    scfg.Bucket,
		interval:  jcfg.SyncInterval,
		cli:       cli,
	}, nil
}

func SendJobToMQ(job app.JobInfoDTO) error {
	body, _ := json.Marshal(job)
	return kafka.Publish(kafkaTopic, &mq.Message{
		Body: body,
	})
}

func (s SyncJobStatus) handle(event mq.Event) error {
	job := app.JobInfoDTO{}
	if err := json.Unmarshal(event.Message().Body, &job); err != nil {
		return err
	}
	s.jobChan <- job
	return nil
}

func Process(s syncjobstatus.SJTInterface) {
	if err := s.Init(); err != nil {
		return
	}

	for {
		jobs := s.GetJobs()
		for _, job := range jobs {
			tJob := job
			go func() {
				defer s.Recovery()(tJob)
				status := s.GetJobStatus(tJob.JobId)
				s.SyncJobStatus(status, tJob)
			}()
		}
		interval := s.GetInterval()
		time.Sleep(time.Duration(interval) * time.Second)
	}
}

func (s SyncJobStatus) Init() error {
	if _, err := kafka.Subscribe(kafkaTopic, s.handle); err != nil {
		s.log.Fatalf("initialize mq failed, err:%v", err)
		return err
	}
	return nil
}

func (s SyncJobStatus) GetInterval() int {
	return s.interval
}

func (s SyncJobStatus) GetJobs() (jobs []app.JobInfoDTO) {
	l := len(s.jobChan)
	for i := 0; i < l; i++ {
		job := <-s.jobChan
		jobs = append(jobs, job)
	}
	return
}

func (s SyncJobStatus) GetJobStatus(jobId string) (r domain.JobDetail) {
	v, err := modelarts.GetJob(s.cli, jobId)
	if err != nil {
		return
	}

	if status, ok := trainingimpl.StatusMap[strings.ToLower(v.Status.Phase)]; ok {
		r.Status = status
	} else {
		r.Status = domain.TrainingStatusAbnormal
	}

	r.Duration = v.Status.Duration
	return
}

func (s SyncJobStatus) SyncJobStatus(status domain.JobDetail, job app.JobInfoDTO) {
	statusStr := status.Status.TrainingStatus()
	if _, ok := StatusEnd[statusStr]; ok {
		return
	}
	if _, ok := StatusContinue[statusStr]; ok {
		_ = SendJobToMQ(job)
		return
	}

	if _, ok := StatusOK[statusStr]; ok {
		sd := SyncData{
			Status: status,
		}

		if status.Status == domain.TrainingStatusCompleted {
			sd.Output = s.handleOutPutFile(job)
		}

		sd.Log = s.handleLogFile(job)
		s.SyncToXiHe(sd)
	}
}

func (s SyncJobStatus) SyncToXiHe(data SyncData) {
	// todo sd sync to xihe
}

func (s SyncJobStatus) handleOutPutFile(job app.JobInfoDTO) string {
	outputKeys := s.listObject(job.OutputDir)
	zipFullPath := s.saveZip(job.JobId, outputKeys)
	defer func() {
		_ = os.Remove(zipFullPath)
	}()
	zipKey := s.PutObject(job.JobId, zipFullPath)
	return s.createObjectUrl(zipKey)
}

func (s SyncJobStatus) handleLogFile(job app.JobInfoDTO) []string {
	logKeys := s.listObject(job.LogDir)
	var logUrls []string
	for _, key := range logKeys {
		url := s.createObjectUrl(key)
		logUrls = append(logUrls, url)
	}
	return logUrls
}

func (s SyncJobStatus) saveZip(jobId string, keys []string) string {
	zipFullPath := zipFileDir + jobId + ".zip"
	fw, err := os.Create(zipFullPath)
	if err != nil {
		panic(err)
	}

	defer fw.Close()
	zw := zip.NewWriter(fw)
	defer func() {
		if err := zw.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	for _, key := range keys {
		sp := strings.Split(key, "/")
		filename := sp[len(sp)-1]

		data := s.GetObject(key)

		w, _ := zw.Create(filename)
		fr := bytes.NewReader(data)

		_, err = io.Copy(w, fr)
		if err != nil {
			panic(err)
		}
	}
	return zipFullPath
}

func (s SyncJobStatus) Recovery() func(app.JobInfoDTO) {
	return func(job app.JobInfoDTO) {
		if err := recover(); err != nil {
			s.log.Panicf("sync job status run time panic: %v", err)
			_ = SendJobToMQ(job)
		}
	}
}

func (s SyncJobStatus) listObject(prefix string) (keys []string) {
	input := &obs.ListObjectsInput{}
	input.Bucket = s.bucket
	input.Prefix = prefix

	output, err := s.obsClient.ListObjects(input)
	if err != nil {
		panic(err)
	}
	for _, val := range output.Contents {
		if val.Key == prefix {
			continue
		}
		keys = append(keys, val.Key)
	}
	return
}

func (s SyncJobStatus) createObjectUrl(key string) string {
	input := &obs.CreateSignedUrlInput{}
	input.Method = obs.HttpMethodGet
	input.Bucket = s.bucket
	input.Key = key
	input.Expires = urlExpire
	output, err := s.obsClient.CreateSignedUrl(input)
	if err != nil {
		panic(err)
	}
	return output.SignedUrl
}

func (s SyncJobStatus) PutObject(jobId, zipFile string) string {
	fi, _ := os.Open(zipFile)
	defer fi.Close()
	input := &obs.PutObjectInput{}
	input.Bucket = s.bucket
	input.Key = zipKeyPrefix + jobId + ".zip"
	input.Body = fi
	if _, err := s.obsClient.PutObject(input); err != nil {
		panic(err)
	}
	return input.Key
}

func (s SyncJobStatus) GetObject(key string) []byte {
	input := &obs.GetObjectInput{}
	input.Bucket = s.bucket
	input.Key = key

	output, err := s.obsClient.GetObject(input)
	if err != nil {
		panic(err)
	}
	defer func() {
		errMsg := output.Body.Close()
		if errMsg != nil {
			panic(errMsg)
		}
	}()
	body, err := ioutil.ReadAll(output.Body)
	if err != nil {
		panic(err)
	}
	return body
}
