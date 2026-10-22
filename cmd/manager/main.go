// Command manager runs the ephemera TTLPolicy controller.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	ephemerav1 "github.com/bhavyakeniya/ephemera/api/v1alpha1"
	"github.com/bhavyakeniya/ephemera/internal/controller"
	"github.com/bhavyakeniya/ephemera/internal/metrics"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(ephemerav1.AddToScheme(scheme))
}

func main() {
	var metricsAddr, probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "address the metrics endpoint binds to")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "address the health probe binds to")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.TTLPolicyReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Metrics: metrics.NewPrometheus(),
		// Read webhook Secrets directly from the API (uncached), so we need only
		// "get" on Secrets in our own namespace, not a cluster-wide Secret watch.
		SecretReader: mgr.GetAPIReader(),
		Namespace:    os.Getenv("POD_NAMESPACE"),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TTLPolicy")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	// SetupSignalHandler wires SIGTERM/SIGINT to context cancellation, which the
	// manager propagates into every Reconcile — the graceful-shutdown path the
	// engine's context handling is built for.
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
