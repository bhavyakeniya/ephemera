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

	// Notify configures optional Slack notifications. When nil, notifications
	// are off and behaviour is unchanged.
	// +optional
	Notify *NotifySpec `json:"notify,omitempty"`
}

// NotifySpec configures Slack notifications for a policy.
type NotifySpec struct {
	// SlackWebhookSecretRef points at a Secret (in the controller's own
	// namespace) holding a Slack Incoming Webhook URL. The URL is a secret and
	// is never stored in the policy spec directly.
	SlackWebhookSecretRef SecretKeyRef `json:"slackWebhookSecretRef"`

	// WarnBefore sends a heads-up this long before a namespace is reaped.
	// Defaults to 24h. The warning is skipped entirely when WarnBefore is >=
	// the policy TTL, since there is then no distinct pre-expiry window.
	// +optional
	WarnBefore *metav1.Duration `json:"warnBefore,omitempty"`

	// DigestCron is a standard 5-field cron expression (e.g. "0 9 * * 1-5") for
	// a periodic summary of this policy. Empty disables the digest.
	// +optional
	DigestCron string `json:"digestCron,omitempty"`
}

// SecretKeyRef references a single key within a Secret.
type SecretKeyRef struct {
	// Name of the Secret.
	Name string `json:"name"`
	// Key within the Secret's data. Defaults to "url" when empty.
	// +optional
	Key string `json:"key,omitempty"`
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

	// RecentReaps holds reap timestamps within the last 24h, used to report
	// "expired in the last 24h" in the digest. It is pruned each reconcile.
	// +optional
	RecentReaps []metav1.Time `json:"recentReaps,omitempty"`

	// Warned lists namespaces already sent a pre-expiry warning, so the warning
	// is sent at most once. Pruned to currently-tracked namespaces.
	// +optional
	Warned []string `json:"warned,omitempty"`

	// LastDigestTime is when the periodic digest was last sent.
	// +optional
	LastDigestTime *metav1.Time `json:"lastDigestTime,omitempty"`
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
