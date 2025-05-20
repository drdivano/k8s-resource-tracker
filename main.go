package main

import (
	"context"
	"log"
	"os"
	"time"
)

const DefaultWatchTimeout = 10 * time.Minute

func main() {
	log.SetOutput(os.Stdout) // Ensure logs go to stdout

	// --- Example Resources to Watch ---
	// Replace these with your actual resource definitions or load them from a config.
	// Ensure these resources exist or will be created in your cluster.
	resourcesToWatch := []ResourceIdentifier{
		// Example 1: A Deployment
		// {Kind: KindDeployment, Namespace: "default", Name: "my-nginx-deployment"},

		// Example 2: A StatefulSet
		// {Kind: KindStatefulSet, Namespace: "default", Name: "my-web-statefulset"},

		// Example 3: A Pod that should complete successfully
		// (Ensure this pod is defined with `restartPolicy: Never` or `OnFailure` and a command that exits)
		// {Kind: KindPod, Namespace: "default", Name: "my-batch-job-pod"},

		// Example 4: A Pod designed to fail and restart (for testing crash loop)
		// {Kind: KindPod, Namespace: "default", Name: "failing-pod"},
	}

	if len(resourcesToWatch) == 0 {
		log.Println("No resources configured to watch. Please edit main.go to add resources.")
		log.Println("Example Usage:")
		log.Println(`  resourcesToWatch = []ResourceIdentifier{`)
		log.Println(`    {Kind: "Deployment", Namespace: "default", Name: "my-app"},`)
		log.Println(`    {Kind: "Pod", Namespace: "batch-jobs", Name: "data-processor-x"},`)
		log.Println(`  }`)
		return
	}
	
	log.Printf("Attempting to watch %d resources.", len(resourcesToWatch))
	for _, r := range resourcesToWatch {
		log.Printf(" - %s", r.String())
	}


	clientset, err := GetKubeClient()
	if err != nil {
		log.Fatalf("🚨 Error creating Kubernetes client: %v", err)
	}
	log.Println("✅ Kubernetes client created successfully.")

	watcher := NewResourceWatcher(clientset)

	timeout := DefaultWatchTimeout
	// You could parse timeout from command-line arguments if needed

	log.Printf("⏳ Starting to watch resources with a timeout of %s...", timeout)
	err = watcher.Watch(context.Background(), resourcesToWatch, timeout)

	if err != nil {
		log.Fatalf("❌ Error watching resources: %v", err)
	} else {
		log.Println("🎉 All specified resources have reached their desired state.")
	}
}