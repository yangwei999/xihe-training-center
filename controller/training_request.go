package controller

import (
	"errors"

	"github.com/opensourceways/xihe-training-center/app"
	"github.com/opensourceways/xihe-training-center/domain"
)

type TrainingResultResp struct {
	URL string `json:"url"`
}

type TrainingCreateRequest struct {
	User          string `json:"user"`
	ProjectId     string `json:"project_id"`
	TrainingId    string `json:"training_id"`
	ProjectName   string `json:"project_name"`
	ProjectRepoId string `json:"project_repo_id"`

	Name string `json:"name"`
	Desc string `json:"desc"`

	CodeDir  string `json:"code_dir"`
	BootFile string `json:"boot_file"`

	Hyperparameters []KeyValue `json:"hyperparameter"`
	Env             []KeyValue `json:"evn"`
	Inputs          []Input    `json:"inputs"`
	EnableAim       bool       `json:"enable_aim"`
	EnableOutput    bool       `json:"enable_output"`

	Compute Compute `json:"compute"`
}

type Compute struct {
	Type    string `json:"type"`
	Version string `json:"version"`
	Flavor  string `json:"flavor"`
}

func (c *Compute) toCompute() (r domain.Compute, err error) {
	if c.Type == "" || c.Version == "" || c.Flavor == "" {
		err = errors.New("invalid compute info")

		return
	}

	if r.Type, err = domain.NewComputeType(c.Type); err != nil {
		return
	}

	if r.Version, err = domain.NewComputeVersion(c.Version); err != nil {
		return
	}

	if r.Flavor, err = domain.NewComputeFlavor(c.Flavor); err != nil {
		return
	}

	return
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (kv *KeyValue) toKeyValue() (r domain.KeyValue, err error) {
	if kv.Key == "" {
		err = errors.New("invalid key value")

		return
	}

	if r.Key, err = domain.NewCustomizedKey(kv.Key); err != nil {
		return
	}

	r.Value, err = domain.NewCustomizedValue(kv.Value)

	return
}

type Input struct {
	Key   string      `json:"key"`
	Value ResourceRef `json:"value"`
}

func (kv *Input) toInput() (r domain.Input, err error) {
	if kv.Key == "" {
		err = errors.New("invalid input")

		return
	}

	if r.Key, err = domain.NewCustomizedKey(kv.Key); err != nil {
		return
	}

	err = kv.Value.toRef(&r.ResourceRef)

	return
}

type ResourceRef struct {
	Owner  string `json:"owner"`
	RepoId string `json:"repo_id"`
	File   string `json:"File"`
}

func (r *ResourceRef) toRef(i *domain.ResourceRef) (err error) {
	if r.Owner == "" || r.RepoId == "" {
		err = errors.New("invalid resource input")

		return
	}

	if i.User, err = domain.NewAccount(r.Owner); err != nil {
		return
	}

	i.RepoId = r.RepoId
	i.File = r.File

	return
}

func (req *TrainingCreateRequest) toCmd() (cmd app.TrainingCreateCmd, err error) {
	if cmd.User, err = domain.NewAccount(req.User); err != nil {
		return
	}

	if cmd.ProjectName, err = domain.NewProjectName(req.ProjectName); err != nil {
		return
	}

	cmd.ProjectId = req.ProjectId
	cmd.TrainingId = req.TrainingId
	cmd.ProjectRepoId = req.ProjectRepoId

	if cmd.Name, err = domain.NewTrainingName(req.Name); err != nil {
		return
	}

	if cmd.Desc, err = domain.NewTrainingDesc(req.Desc); err != nil {
		return
	}

	if cmd.CodeDir, err = domain.NewDirectory(req.CodeDir); err != nil {
		return
	}

	if cmd.BootFile, err = domain.NewFilePath(req.BootFile); err != nil {
		return
	}

	if cmd.Compute, err = req.Compute.toCompute(); err != nil {
		return
	}

	if cmd.Hyperparameters, err = req.toKeyValue(req.Hyperparameters); err != nil {
		return
	}

	if cmd.Env, err = req.toKeyValue(req.Env); err != nil {
		return
	}

	if cmd.Inputs, err = req.toInputs(req.Inputs); err != nil {
		return
	}

	cmd.EnableAim = req.EnableAim
	cmd.EnableOutput = req.EnableOutput

	return
}

func (req *TrainingCreateRequest) toKeyValue(kv []KeyValue) (r []domain.KeyValue, err error) {
	n := len(kv)
	if n == 0 {
		return nil, nil
	}

	r = make([]domain.KeyValue, n)
	for i := range kv {
		if r[i], err = kv[i].toKeyValue(); err != nil {
			return
		}
	}

	return
}

func (req *TrainingCreateRequest) toInputs(kv []Input) (r []domain.Input, err error) {
	n := len(kv)
	if n == 0 {
		return nil, nil
	}

	r = make([]domain.Input, n)
	for i := range kv {
		if r[i], err = kv[i].toInput(); err != nil {
			return
		}
	}

	return
}
