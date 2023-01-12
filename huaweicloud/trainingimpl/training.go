package trainingimpl

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chnsz/golangsdk"

	"github.com/opensourceways/xihe-training-center/domain"
	"github.com/opensourceways/xihe-training-center/domain/training"
	"github.com/opensourceways/xihe-training-center/huaweicloud/client"
	"github.com/opensourceways/xihe-training-center/huaweicloud/modelarts"
)

const (
	obsDelimiter = "/"
)

var statusMap = map[string]domain.TrainingStatus{
	"failed":      domain.TrainingStatusFailed,
	"pending":     domain.TrainingStatusPending,
	"running":     domain.TrainingStatusRunning,
	"creating":    domain.TrainingStatusCreating,
	"abnormal":    domain.TrainingStatusAbnormal,
	"completed":   domain.TrainingStatusCompleted,
	"terminated":  domain.TrainingStatusTerminated,
	"terminating": domain.TrainingStatusTerminating,
}

func NewTraining(cfg *Config) (training.Training, error) {
	s := "modelarts"
	mc := &cfg.Modelarts
	v := client.Config{
		AccessKey:  mc.AccessKey,
		SecretKey:  mc.SecretKey,
		TenantName: mc.ProjectName,
		TenantID:   mc.ProjectId,
		Region:     mc.Region,
		Endpoints: map[string]string{
			s: mc.Endpoint,
		},
		IdentityEndpoint: fmt.Sprintf("https://iam.%s.myhuaweicloud.com:443/v3", mc.Region),
	}
	if err := v.LoadAndValidate(); err != nil {
		return nil, err
	}

	cli, err := v.NewServiceClient(s, client.ServiceCatalog{
		Version: "v2",
	})
	if err != nil {
		return nil, err
	}

	h, err := newHelper(cfg)
	if err != nil {
		return nil, err
	}

	return trainingImpl{
		cli:    cli,
		config: cfg.Train,
		helper: h,

		obsRepoPath: filepath.Join(
			obsDelimiter, cfg.OBS.Bucket, cfg.SyncAndUpload.RepoPath,
		),
		obsRepoPathPrefix: obsDelimiter + cfg.OBS.Bucket + obsDelimiter,
	}, nil
}

type trainingImpl struct {
	cli    *golangsdk.ServiceClient
	config TrainingConfig

	obsRepoPath       string
	obsRepoPathPrefix string

	*helper
}

func (impl trainingImpl) genJobParameter(t *domain.UserTraining, opt *modelarts.JobCreateOption) {
	if n := len(t.Hyperparameters); n > 0 {
		p := make([]modelarts.ParameterOption, n)

		for i, v := range t.Hyperparameters {
			s := ""
			if v.Value != nil {
				s = v.Value.CustomizedValue()
			}

			p[i] = modelarts.ParameterOption{
				Name:  v.Key.CustomizedKey(),
				Value: s,
			}
		}

		opt.Algorithm.Parameters = p
	}

	if n := len(t.Env); n > 0 {
		m := make(map[string]string)

		for _, v := range t.Env {
			s := ""
			if v.Value != nil {
				s = v.Value.CustomizedValue()
			}

			m[v.Key.CustomizedKey()] = s
		}

		opt.Algorithm.Environments = m
	}
}

func (impl trainingImpl) Create(t *domain.UserTraining) (info domain.JobInfo, err error) {
	desc := ""
	if t.Desc != nil {
		desc = t.Desc.TrainingDesc()
	}

	cfg := &impl.config
	obs := filepath.Join(impl.obsRepoPath, t.ToRepoPath())

	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	logDir := filepath.Join(obs, cfg.LogDir, timestamp) + obsDelimiter
	aimDir := filepath.Join(obs, cfg.AimDir, timestamp) + obsDelimiter
	outputDir := filepath.Join(obs, cfg.OutputDir, timestamp) + obsDelimiter

	outputs := []modelarts.InputOutputOption{}
	if t.EnableAim {
		outputs = append(outputs, modelarts.InputOutputOption{
			Name: cfg.AimKey,
			Remote: modelarts.RemoteOption{
				OBS: modelarts.OBSOption{
					OBSURL: aimDir,
				},
			},
		})
	}
	if t.EnableOutput {
		outputs = append(outputs, modelarts.InputOutputOption{
			Name: cfg.OutputKey,
			Remote: modelarts.RemoteOption{
				OBS: modelarts.OBSOption{
					OBSURL: outputDir,
				},
			},
		})
	}

	opt := modelarts.JobCreateOption{
		Kind: "job",
		Metadata: modelarts.MetadataOption{
			Name: t.ProjectRepoId + "_" + t.Name.TrainingName(),
			Desc: desc,
		},
		Algorithm: modelarts.AlgorithmOption{
			CodeDir:  filepath.Join(obs, t.CodeDir.Directory()) + obsDelimiter,
			BootFile: filepath.Join(obs, t.CodeDir.Directory(), t.BootFile.FilePath()),
			Engine: modelarts.EngineOption{
				EngineName:    t.Compute.Type.ComputeType(),
				EngineVersion: t.Compute.Version.ComputeVersion(),
			},
			Outputs: outputs,
		},
		Spec: modelarts.SpecOption{
			Resource: modelarts.ResourceOption{
				FlavorId:  t.Compute.Flavor.ComputeFlavor(),
				NodeCount: 1,
			},
			LogExportPath: modelarts.LogExportPathOption{
				OBSURL: logDir,
			},
		},
	}

	if len(t.Inputs) > 0 {
		opt.Algorithm.Inputs = impl.genInputOption(t.Inputs)
	}

	impl.genJobParameter(t, &opt)

	info.JobId, err = modelarts.CreateJob(impl.cli, opt)

	if err == nil {
		p := impl.obsRepoPathPrefix
		info.LogDir = strings.TrimPrefix(logDir, p)

		if t.EnableAim {
			info.AimDir = strings.TrimPrefix(aimDir, p)
		}
		if t.EnableOutput {
			info.OutputDir = strings.TrimPrefix(outputDir, p)
		}
	}

	return
}

func (impl trainingImpl) genInputOption(kv []domain.Input) []modelarts.InputOutputOption {
	r := make([]modelarts.InputOutputOption, len(kv))

	for i, v := range kv {
		r[i] = modelarts.InputOutputOption{
			Name: v.Key.CustomizedKey(),
			Remote: modelarts.RemoteOption{
				OBS: modelarts.OBSOption{
					// v.Value maybe a directory.
					OBSURL: impl.obsRepoPath + obsDelimiter + v.ToPath(),
				},
			},
		}
	}

	return r
}

func (impl trainingImpl) Delete(jobId string) error {
	return modelarts.DeleteJob(impl.cli, jobId)
}

func (impl trainingImpl) GetDetail(jobId string) (r domain.JobDetail, err error) {
	v, err := modelarts.GetJob(impl.cli, jobId)
	if err != nil {
		return
	}

	if status, ok := statusMap[strings.ToLower(v.Status.Phase)]; ok {
		r.Status = status
	} else {
		r.Status = domain.TrainingStatusFailed
	}

	// convert millisecond to second
	r.Duration = v.Status.Duration / 1000

	return
}

func (impl trainingImpl) Terminate(jobId string) error {
	return modelarts.TerminateJob(impl.cli, jobId)
}

func (impl trainingImpl) GetLogDownloadURL(jobId string) (string, error) {
	return modelarts.GetLogDownloadURL(impl.cli, jobId)
}
