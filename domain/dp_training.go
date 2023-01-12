package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const pathSpliter = "/"

var (
	reName      = regexp.MustCompile("^[a-zA-Z0-9_-]+$")
	reDirectory = regexp.MustCompile("^[a-zA-Z0-9_/-]+$")
	reFilePath  = regexp.MustCompile("^[a-zA-Z0-9_/.-]+$")

	TrainingStatusFailed      = trainingStatus("Failed")
	TrainingStatusPending     = trainingStatus("Pending")
	TrainingStatusRunning     = trainingStatus("Running")
	TrainingStatusCreating    = trainingStatus("Creating")
	TrainingStatusAbnormal    = trainingStatus("Abnormal")
	TrainingStatusCompleted   = trainingStatus("Completed")
	TrainingStatusTerminated  = trainingStatus("Terminated")
	TrainingStatusTerminating = trainingStatus("Terminating")

	trainingDoneStatus = map[string]bool{
		"Failed":     true,
		"Abnormal":   true,
		"Completed":  true,
		"Terminated": true,
	}
)

// Account
type Account interface {
	Account() string
}

func NewAccount(v string) (Account, error) {
	if v == "" || strings.ToLower(v) == "root" || !reName.MatchString(v) {
		return nil, errors.New("invalid user name")
	}

	return dpAccount(v), nil
}

type dpAccount string

func (r dpAccount) Account() string {
	return string(r)
}

// TrainingName
type TrainingName interface {
	TrainingName() string
}

func NewTrainingName(v string) (TrainingName, error) {
	max := config.MaxTrainingNameLength
	min := config.MinTrainingNameLength

	if n := len(v); n > max || n < min {
		return nil, fmt.Errorf("name's length should be between %d to %d", min, max)
	}

	if !reName.MatchString(v) {
		return nil, errors.New("invalid name")
	}

	return trainingName(v), nil
}

type trainingName string

func (r trainingName) TrainingName() string {
	return string(r)
}

// TrainingDesc
type TrainingDesc interface {
	TrainingDesc() string
}

func NewTrainingDesc(v string) (TrainingDesc, error) {
	if v == "" {
		return nil, nil
	}

	max := config.MaxTrainingDescLength
	if len(v) > max {
		return nil, fmt.Errorf("the length of desc should be less than %d", max)
	}

	return trainingDesc(v), nil
}

type trainingDesc string

func (r trainingDesc) TrainingDesc() string {
	return string(r)
}

// Directory
type Directory interface {
	Directory() string
}

func NewDirectory(v string) (Directory, error) {
	if v == "" {
		return directory(""), nil
	}

	if !reDirectory.MatchString(v) {
		return nil, errors.New("invalid directory")
	}

	return directory(v), nil
}

type directory string

func (r directory) Directory() string {
	return string(r)
}

// FilePath
type FilePath interface {
	FilePath() string
}

func NewFilePath(v string) (FilePath, error) {
	if v == "" {
		return nil, errors.New("empty file path")
	}

	if !reFilePath.MatchString(v) {
		return nil, errors.New("invalid filePath")
	}

	return filePath(v), nil
}

type filePath string

func (r filePath) FilePath() string {
	return string(r)
}

// ComputeType
type ComputeType interface {
	ComputeType() string
}

func NewComputeType(v string) (ComputeType, error) {
	if v == "" {
		return nil, errors.New("empty compute type")
	}

	return computeType(v), nil
}

type computeType string

func (r computeType) ComputeType() string {
	return string(r)
}

// ComputeVersion
type ComputeVersion interface {
	ComputeVersion() string
}

func NewComputeVersion(v string) (ComputeVersion, error) {
	if v == "" {
		return nil, errors.New("empty compute version")
	}

	return computeVersion(v), nil
}

type computeVersion string

func (r computeVersion) ComputeVersion() string {
	return string(r)
}

// ComputeFlavor
type ComputeFlavor interface {
	ComputeFlavor() string
}

func NewComputeFlavor(v string) (ComputeFlavor, error) {
	if v == "" {
		return nil, errors.New("empty compute flavor")
	}

	return computeFlavor(v), nil
}

type computeFlavor string

func (r computeFlavor) ComputeFlavor() string {
	return string(r)
}

// CustomizedKey
type CustomizedKey interface {
	CustomizedKey() string
}

func NewCustomizedKey(v string) (CustomizedKey, error) {
	if v == "" {
		return nil, errors.New("empty key")
	}

	return customizedKey(v), nil
}

type customizedKey string

func (r customizedKey) CustomizedKey() string {
	return string(r)
}

// CustomizedValue
type CustomizedValue interface {
	CustomizedValue() string
}

func NewCustomizedValue(v string) (CustomizedValue, error) {
	if v == "" {
		return nil, nil
	}

	return customizedValue(v), nil
}

type customizedValue string

func (r customizedValue) CustomizedValue() string {
	return string(r)
}

// TrainingStatus
type TrainingStatus interface {
	TrainingStatus() string
	IsDone() bool
	IsSuccess() bool
}

type trainingStatus string

func (s trainingStatus) TrainingStatus() string {
	return string(s)
}

func (s trainingStatus) IsDone() bool {
	return trainingDoneStatus[string(s)]
}

func (s trainingStatus) IsSuccess() bool {
	return string(s) == TrainingStatusCompleted.TrainingStatus()
}
