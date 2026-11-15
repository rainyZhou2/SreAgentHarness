package plugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrDescriptorInvalid = errors.New("plugin descriptor invalid")
	ErrIncompatible      = errors.New("plugin incompatible")
)

type CompatibilityPolicy string

const (
	ExactContract   CompatibilityPolicy = "exact"
	CompatibleMajor CompatibilityPolicy = "compatible_major"
)

type DataClass string

const (
	DataPublic       DataClass = "public"
	DataInternal     DataClass = "internal"
	DataConfidential DataClass = "confidential"
	DataRestricted   DataClass = "restricted"
)

type PermissionScope struct {
	Services []string `json:"services,omitempty"`
}

type ResourceBudget struct {
	StartTimeout   time.Duration `json:"start_timeout"`
	ReadyTimeout   time.Duration `json:"ready_timeout"`
	DrainTimeout   time.Duration `json:"drain_timeout"`
	StopTimeout    time.Duration `json:"stop_timeout"`
	HealthTimeout  time.Duration `json:"health_timeout"`
	HealthInterval time.Duration `json:"health_interval"`
	MaxTasks       int           `json:"max_tasks"`
	MaxConcurrent  int           `json:"max_concurrent"`
}

type ErrorContract struct {
	RetryableCodes []string `json:"retryable_codes,omitempty"`
	PublicCodes    []string `json:"public_codes,omitempty"`
	UnhealthyCodes []string `json:"unhealthy_codes,omitempty"`
}

type ObservabilityContract struct {
	Metrics   []string `json:"metrics,omitempty"`
	LogFields []string `json:"log_fields,omitempty"`
}

type Descriptor struct {
	Kind                string                `json:"kind"`
	Name                string                `json:"name"`
	Version             string                `json:"version"`
	ContractVersion     string                `json:"contract_version"`
	MinimumCoreVersion  string                `json:"minimum_core_version"`
	CompatibilityPolicy CompatibilityPolicy   `json:"compatibility_policy"`
	Description         string                `json:"description,omitempty"`
	Capabilities        []string              `json:"capabilities,omitempty"`
	ConfigurationSchema json.RawMessage       `json:"configuration_schema,omitempty"`
	PermissionScope     PermissionScope       `json:"permission_scope"`
	DataClasses         []DataClass           `json:"data_classes,omitempty"`
	ResourceBudget      ResourceBudget        `json:"resource_budget"`
	ErrorContract       ErrorContract         `json:"error_contract"`
	Observability       ObservabilityContract `json:"observability"`
}

func DefaultResourceBudget() ResourceBudget {
	return ResourceBudget{
		StartTimeout:   30 * time.Second,
		ReadyTimeout:   30 * time.Second,
		DrainTimeout:   20 * time.Second,
		StopTimeout:    10 * time.Second,
		HealthTimeout:  5 * time.Second,
		HealthInterval: 30 * time.Second,
		MaxConcurrent:  1,
	}
}

var (
	pluginNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	errorCodePattern  = regexp.MustCompile(`^[a-z][a-z0-9_.-]*$`)
	semVersionPattern = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-([0-9A-Za-z.-]+))?$`)
)

func (d Descriptor) Validate() error {
	if !pluginNamePattern.MatchString(d.Name) {
		return descriptorFieldError("name", d.Name)
	}
	for field, value := range map[string]string{
		"version": d.Version, "contract_version": d.ContractVersion, "minimum_core_version": d.MinimumCoreVersion,
	} {
		if _, err := parseSemanticVersion(value); err != nil {
			return fmt.Errorf("%w: %s: %v", ErrDescriptorInvalid, field, err)
		}
	}
	switch d.CompatibilityPolicy {
	case ExactContract, CompatibleMajor:
	default:
		return descriptorFieldError("compatibility_policy", d.CompatibilityPolicy)
	}
	if err := uniqueNames("capability", d.Capabilities); err != nil {
		return err
	}
	if err := uniqueNames("service", d.PermissionScope.Services); err != nil {
		return err
	}
	seenData := map[DataClass]bool{}
	for _, class := range d.DataClasses {
		switch class {
		case DataPublic, DataInternal, DataConfidential, DataRestricted:
		default:
			return descriptorFieldError("data_class", class)
		}
		if seenData[class] {
			return descriptorFieldError("data_class", "duplicate "+string(class))
		}
		seenData[class] = true
	}
	if len(bytes.TrimSpace(d.ConfigurationSchema)) > 0 {
		var schema map[string]any
		if err := json.Unmarshal(d.ConfigurationSchema, &schema); err != nil || schema == nil {
			return descriptorFieldError("configuration_schema", "must be a JSON object")
		}
	}
	for field, timeout := range map[string]time.Duration{
		"start_timeout":   d.ResourceBudget.StartTimeout,
		"ready_timeout":   d.ResourceBudget.ReadyTimeout,
		"drain_timeout":   d.ResourceBudget.DrainTimeout,
		"stop_timeout":    d.ResourceBudget.StopTimeout,
		"health_timeout":  d.ResourceBudget.HealthTimeout,
		"health_interval": d.ResourceBudget.HealthInterval,
	} {
		if timeout <= 0 {
			return descriptorFieldError(field, "must be positive")
		}
	}
	if d.ResourceBudget.MaxTasks < 0 {
		return descriptorFieldError("max_tasks", "must not be negative")
	}
	if d.ResourceBudget.MaxConcurrent < 0 {
		return descriptorFieldError("max_concurrent", "must not be negative")
	}
	for _, codes := range [][]string{d.ErrorContract.RetryableCodes, d.ErrorContract.PublicCodes, d.ErrorContract.UnhealthyCodes} {
		for _, code := range codes {
			if !errorCodePattern.MatchString(code) {
				return descriptorFieldError("error_code", code)
			}
		}
	}
	if err := uniqueNames("metric", d.Observability.Metrics); err != nil {
		return err
	}
	return uniqueNames("log_field", d.Observability.LogFields)
}

func (d Descriptor) CheckCompatibility(coreVersion, kindContract string) error {
	if err := d.Validate(); err != nil {
		return err
	}
	pluginContract, _ := parseSemanticVersion(d.ContractVersion)
	hostContract, err := parseSemanticVersion(kindContract)
	if err != nil {
		return fmt.Errorf("%w: kind contract %q: %v", ErrIncompatible, kindContract, err)
	}
	switch d.CompatibilityPolicy {
	case ExactContract:
		if pluginContract.compare(hostContract) != 0 {
			return incompatibleVersion("contract", d.ContractVersion, kindContract)
		}
	case CompatibleMajor:
		if pluginContract.major != hostContract.major || pluginContract.compare(hostContract) > 0 {
			return incompatibleVersion("contract", d.ContractVersion, kindContract)
		}
	}
	minimum, _ := parseSemanticVersion(d.MinimumCoreVersion)
	if coreVersion == "dev" {
		if minimum.isZero() {
			return nil
		}
		return incompatibleVersion("minimum core", d.MinimumCoreVersion, coreVersion)
	}
	core, err := parseSemanticVersion(coreVersion)
	if err != nil {
		return fmt.Errorf("%w: core version %q: %v", ErrIncompatible, coreVersion, err)
	}
	if core.compare(minimum) < 0 {
		return incompatibleVersion("minimum core", d.MinimumCoreVersion, coreVersion)
	}
	return nil
}

func uniqueNames(field string, values []string) error {
	seen := map[string]bool{}
	for _, value := range values {
		if !pluginNamePattern.MatchString(value) || seen[value] {
			return descriptorFieldError(field, value)
		}
		seen[value] = true
	}
	return nil
}

func descriptorFieldError(field string, value any) error {
	return fmt.Errorf("%w: %s %v", ErrDescriptorInvalid, field, value)
}

func incompatibleVersion(field, required, actual string) error {
	return fmt.Errorf("%w: %s %s does not accept %s", ErrIncompatible, field, required, actual)
}

type semanticVersion struct {
	major, minor, patch uint64
	prerelease          string
}

func (v semanticVersion) isZero() bool {
	return v.major == 0 && v.minor == 0 && v.patch == 0 && v.prerelease == ""
}

func (v semanticVersion) compare(other semanticVersion) int {
	for _, pair := range [][2]uint64{{v.major, other.major}, {v.minor, other.minor}, {v.patch, other.patch}} {
		if pair[0] < pair[1] {
			return -1
		}
		if pair[0] > pair[1] {
			return 1
		}
	}
	if v.prerelease == other.prerelease {
		return 0
	}
	if v.prerelease == "" {
		return 1
	}
	if other.prerelease == "" {
		return -1
	}
	left := strings.Split(v.prerelease, ".")
	right := strings.Split(other.prerelease, ".")
	for i := 0; i < len(left) && i < len(right); i++ {
		if left[i] == right[i] {
			continue
		}
		ln, le := strconv.ParseUint(left[i], 10, 64)
		rn, re := strconv.ParseUint(right[i], 10, 64)
		switch {
		case le == nil && re == nil && ln < rn:
			return -1
		case le == nil && re == nil && ln > rn:
			return 1
		case le == nil:
			return -1
		case re == nil:
			return 1
		case left[i] < right[i]:
			return -1
		default:
			return 1
		}
	}
	if len(left) < len(right) {
		return -1
	}
	return 1
}

func parseSemanticVersion(raw string) (semanticVersion, error) {
	matches := semVersionPattern.FindStringSubmatch(raw)
	if matches == nil {
		return semanticVersion{}, errors.New("expected vMAJOR.MINOR.PATCH with optional prerelease")
	}
	parts := make([]uint64, 3)
	for i := range parts {
		value, err := strconv.ParseUint(matches[i+1], 10, 64)
		if err != nil {
			return semanticVersion{}, err
		}
		parts[i] = value
	}
	prerelease := matches[4]
	if prerelease != "" {
		for _, identifier := range strings.Split(prerelease, ".") {
			if identifier == "" {
				return semanticVersion{}, errors.New("empty prerelease identifier")
			}
			if len(identifier) > 1 && identifier[0] == '0' {
				if _, err := strconv.ParseUint(identifier, 10, 64); err == nil {
					return semanticVersion{}, errors.New("numeric prerelease has leading zero")
				}
			}
		}
	}
	return semanticVersion{major: parts[0], minor: parts[1], patch: parts[2], prerelease: prerelease}, nil
}
