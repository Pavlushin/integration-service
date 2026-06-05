package workflow

import "fmt"

type Registry struct {
	workflows map[string]Workflow
}

func NewRegistry(workflows ...Workflow) (*Registry, error) {
	items := make(map[string]Workflow, len(workflows))

	for _, item := range workflows {
		workflowType := item.Type()
		if workflowType == "" {
			return nil, fmt.Errorf("workflow type is required")
		}
		if _, exists := items[workflowType]; exists {
			return nil, fmt.Errorf("workflow already registered: %s", workflowType)
		}

		items[workflowType] = item
	}

	return &Registry{
		workflows: items,
	}, nil
}

func MustNewRegistry(workflows ...Workflow) *Registry {
	registry, err := NewRegistry(workflows...)
	if err != nil {
		panic(fmt.Errorf("create workflow registry: %w", err))
	}

	return registry
}

func (r *Registry) WorkflowByType(workflowType string) (Workflow, error) {
	if r == nil {
		return nil, fmt.Errorf("workflow registry is nil")
	}

	workflow, ok := r.workflows[workflowType]
	if !ok {
		return nil, fmt.Errorf("workflow not found: %s", workflowType)
	}

	return workflow, nil
}
