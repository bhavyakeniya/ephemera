package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TTLPolicySpec defines which namespaces to watch and how long they may live.
type TTLPolicySpec struct {
	// Selector picks the namespaces this policy governs. An empty selector is
	// rejected (the policy is marked NotReady and nothing is reaped) rather than
	// matching everything — you must opt namespaces in explicitly.
	Selector metav1.LabelSelector `json:"selector"`

	// TTL is how long after a namespace's creation it may live before reaping.
	// +kubebuilder:validation:Required
	TTL metav1.Duration `json:"ttl"`

	// DryRun, when true, logs and reports what would be reaped without deleting.
	// +kubebuilder:default=true
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// Workers caps concurrent deletions. Defaults to 4 when unset.
	// +kubebuilder:validation:Minimum=1
	// +optional
	Workers *int `json:"workers,omitempty"`
}

// TTLPolicyStatus reports the last observed reaping state.
type TTLPolicyStatus struct {
	// Conditions follow the standard Kubernetes condition convention. The
	// "Ready" condition reflects whether the last reconcile succeeded.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Tracked is the number of namespaces currently matched by the selector.
	// +optional
	Tracked int `json:"tracked,omitempty"`

	// Reaped is the cumulative count of namespaces reaped by this policy.
	// +optional
	Reaped int `json:"reaped,omitempty"`

	// LastReapTime is when the controller last ran a reap pass for this policy.
	// +optional
	LastReapTime *metav1.Time `json:"lastReapTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,shortName=ttl
// +kubebuilder:printcolumn:name="TTL",type=string,JSONPath=`.spec.ttl`
// +kubebuilder:printcolumn:name="DryRun",type=boolean,JSONPath=`.spec.dryRun`
// +kubebuilder:printcolumn:name="Tracked",type=integer,JSONPath=`.status.tracked`
// +kubebuilder:printcolumn:name="Reaped",type=integer,JSONPath=`.status.reaped`

// TTLPolicy is the Schema for the ttlpolicies API. It is cluster-scoped because
// it governs namespaces, which are themselves cluster-scoped.
type TTLPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TTLPolicySpec   `json:"spec,omitempty"`
	Status TTLPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TTLPolicyList contains a list of TTLPolicy.
type TTLPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TTLPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TTLPolicy{}, &TTLPolicyList{})
}
