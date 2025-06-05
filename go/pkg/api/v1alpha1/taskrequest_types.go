package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// EDIT: adjust the group and version to match your installation.
const (
	Group   = "scheduler.rcme.ai"
	Version = "v1alpha1"
)

var SchemeGroupVersion = schema.GroupVersion{Group: Group, Version: Version}

// -----------------------------------------------------------------------------
// 1. CRD Go types (matching the YAML definition)
// -----------------------------------------------------------------------------

type TaskRequestStatus struct {
	// Fill in once you need status fields
}

type TaskRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TaskRequestSpec   `json:"spec,omitempty"`
	Status            TaskRequestStatus `json:"status,omitempty"`
}

type TaskRequestSpec struct {
	Priority int               `json:"priority"`
	Payload  map[string]string `json:"payload"` // arbitrary key-value pairs (image, args, etc.)
}

// +kubebuilder:object:root=true
// TaskRequestList is required for the informer cache.

type TaskRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TaskRequest `json:"items"`
}

// AddToScheme registers TaskRequest and TaskRequestList to a runtime.Scheme so controller-runtime can serialise/deserialise them.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(SchemeGroupVersion,
		&TaskRequest{},
		&TaskRequestList{},
	)
	metav1.AddToGroupVersion(s, SchemeGroupVersion)
	return nil
}

// DeepCopyObject implementations.
func (in *TaskRequest) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (in *TaskRequestList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}
