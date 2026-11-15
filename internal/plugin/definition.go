package plugin

import (
	"context"
	"errors"
)

type Definition[T any] struct {
	Descriptor     Descriptor
	ValidateConfig func(Config) error
	Initialize     func(context.Context, *Host, Config) (Component[T], error)
}

func Define[T any](
	name, version, doc string,
	validate func(Config) error,
	initialize func(context.Context, *Host, Config) (Component[T], error),
) Definition[T] {
	if validate == nil {
		validate = rejectConfiguration
	}
	return Definition[T]{
		Descriptor: Descriptor{
			Name:                name,
			Version:             version,
			ContractVersion:     "v1.0.0",
			MinimumCoreVersion:  "v0.0.0",
			CompatibilityPolicy: CompatibleMajor,
			Description:         doc,
			ResourceBudget:      DefaultResourceBudget(),
		},
		ValidateConfig: validate,
		Initialize:     initialize,
	}
}

func StaticDefinition[T any](
	name, version, doc string,
	validate func(Config) error,
	initialize func(context.Context, *Host, Config) (T, error),
) Definition[T] {
	return Define(name, version, doc, validate,
		func(ctx context.Context, host *Host, config Config) (Component[T], error) {
			value, err := initialize(ctx, host, config)
			if err != nil {
				return Component[T]{}, err
			}
			return Static(value), nil
		})
}

func BackgroundDefinition(
	name, version, doc string,
	validate func(Config) error,
	initialize func(context.Context, *Host, Config) (Task, error),
) Definition[Task] {
	definition := Define(name, version, doc, validate,
		func(ctx context.Context, host *Host, config Config) (Component[Task], error) {
			task, err := initialize(ctx, host, config)
			if err != nil {
				return Component[Task]{}, err
			}
			return Background(task, name), nil
		})
	definition.Descriptor.ResourceBudget.MaxTasks = 1
	return definition
}

func StrictConfig[C any]() func(Config) error {
	return func(config Config) error {
		var settings C
		return config.Decode(&settings)
	}
}

func rejectConfiguration(config Config) error {
	if config.Empty() {
		return nil
	}
	return errors.New("config: plugin does not accept configuration")
}
