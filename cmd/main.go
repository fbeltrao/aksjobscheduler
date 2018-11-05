package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	scheduler "github.com/fbeltrao/aksjobscheduler/scheduler"
	"github.com/satori/go.uuid"
)

func main() {

	// Gets default parallelism from env
	defaultParallelism := 1
	if envParallelism, _ := strconv.Atoi(os.Getenv("PARALLELISM")); envParallelism > 0 {
		defaultParallelism = envParallelism
	}

	// Gets default node selector host name for ACI
	defaultAciSelectorHostName := os.Getenv("ACIHOSTNAME")
	if len(defaultAciSelectorHostName) == 0 {
		defaultAciSelectorHostName = "virtual-kubelet-virtual-kubelet-linux-westeurope"
	}

	// verify if the cluster config file is being passed
	var kubeconfig *string
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}

	useAci := flag.Bool("aci", false, "create jobs in aci or not")
	parallelism := flag.Int("parallelism", defaultParallelism, "job parallelism (how many instances should exist at a time")
	completions := flag.Int("completions", 1, "job completions (how many times it must be performed)")
	aciSelectorHostName := flag.String("acihostname", defaultAciSelectorHostName, "ACI node selector host name (example: virtual-kubelet-virtual-kubelet-linux-westeurope)")

	flag.Parse()

	k8scheduler, err := scheduler.NewScheduler(*kubeconfig)
	if err != nil {
		panic(err.Error())
	}
	//k8scheduler.ACIEnabled = *useAci
	k8scheduler.ACISelectorHostName = *aciSelectorHostName

	for {
		jobs, err := k8scheduler.ListJobs()
		if err != nil {
			panic(err.Error())
		}

		var activeJobPods int32
		for _, jobItem := range jobs.Items {
			activeJobPods += jobItem.Status.Active
		}

		fmt.Printf("There are %d jobs in the cluster, %d being active\n", len(jobs.Items), activeJobPods)

		if activeJobPods == 0 {

			jobUUID, err := uuid.NewV4()
			if err != nil {
				panic(err.Error())
			}

			var jobID string
			if *useAci {
				jobID = fmt.Sprintf("job-aci-%s", jobUUID.String())
			} else {
				jobID = fmt.Sprintf("job-%s", jobUUID.String())
			}

			jobDetail := scheduler.NewJobDetail{
				JobID:       jobID,
				JobName:     "pi pearl calculation",
				ImageName:   "pi",
				Commands:    []string{"perl", "-Mbignum=bpi", "-wle", "'print bpi(2000)'"},
				Image:       "perl",
				Parallelism: *parallelism,
				Completions: *completions,
				RequiresACI: *useAci,
			}

			jobCreateResult, err := k8scheduler.NewJob(&jobDetail)
			if err != nil {
				panic(err.Error())
			}

			fmt.Printf("Created new job: %s", jobCreateResult.Name)
		}

		time.Sleep(10 * time.Second)
	}
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
