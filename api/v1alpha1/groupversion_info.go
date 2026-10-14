// Package v1alpha1 contains the TTLPolicy custom resource definition.
// +kubebuilder:object:generate=true
// +groupName=ephemera.dev
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the API group and version for this CRD.
var GroupVersion = schema.GroupVersion{Group: "ephemera.dev", Version: "v1alpha1"}

// SchemeBuilder registers this CRD's types with a runtime scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the types in this group-version to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme
