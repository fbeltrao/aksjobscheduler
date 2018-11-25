/*
 This is work in progress
 IT DOES NOT WORK!
*/
package main

import (
	"context"
	"fmt"

	batchARM "github.com/Azure/azure-sdk-for-go/services/batch/2018-08-01.7.0/batch"
	log "github.com/sirupsen/logrus"
)

// func getAccountClient() batchARM.AccountClient {
// 	accountClient := batchARM.NewAccountClient(config.SubscriptionID())
// 	auth, _ := iam.GetResourceManagementAuthorizer()
// 	accountClient.Authorizer = auth
// 	accountClient.AddToUserAgent(config.UserAgent())
// 	return accountClient
// }

// func getPoolClient(accountName, accountLocation string) batch.PoolClient {
// 	poolClient := batch.NewPoolClientWithBaseURI(getBatchBaseURL(accountName, accountLocation))
// 	auth, _ := iam.GetBatchAuthorizer()
// 	poolClient.Authorizer = auth
// 	poolClient.AddToUserAgent(config.UserAgent())
// 	poolClient.RequestInspector = fixContentTypeInspector()
// 	return poolClient
// }

// func getJobClient(accountName, accountLocation string) batch.JobClient {
// 	jobClient := batch.NewJobClientWithBaseURI(getBatchBaseURL(accountName, accountLocation))
// 	auth, _ := iam.GetBatchAuthorizer()
// 	jobClient.Authorizer = auth
// 	jobClient.AddToUserAgent(config.UserAgent())
// 	jobClient.RequestInspector = fixContentTypeInspector()
// 	return jobClient
// }

// func getTaskClient(accountName, accountLocation string) batch.TaskClient {
// 	taskClient := batch.NewTaskClientWithBaseURI(getBatchBaseURL(accountName, accountLocation))
// 	auth, _ := iam.GetBatchAuthorizer()
// 	taskClient.Authorizer = auth
// 	taskClient.AddToUserAgent(config.UserAgent())
// 	taskClient.RequestInspector = fixContentTypeInspector()
// 	return taskClient
// }

// func getFileClient(accountName, accountLocation string) batch.FileClient {
// 	fileClient := batch.NewFileClientWithBaseURI(getBatchBaseURL(accountName, accountLocation))
// 	auth, _ := iam.GetBatchAuthorizer()
// 	fileClient.Authorizer = auth
// 	fileClient.AddToUserAgent(config.UserAgent())
// 	fileClient.RequestInspector = fixContentTypeInspector()
// 	return fileClient
// }

func main() {
	log.Infof("Starting Batch Connector")

	jobID := ""
	jobName := ""
	poolID := ""
	workerImage := ""
	completions := 10
	TRUE := true

	jobSpecification := batchARM.JobSpecification{
		PoolInfo: &batchARM.PoolInformation{
			PoolID: &poolID,
		},
		UsesTaskDependencies: &TRUE,
	}

	// v := batchARM.NewJobClientWithBaseURI()
	// batchARM
	// v.Authorizer :=

	jobClient := batchARM.NewJobScheduleClient()
	ctx := context.Background()
	_, err := jobClient.Add(ctx, batchARM.JobScheduleAddParameter{
		DisplayName:      &jobName,
		ID:               &jobID,
		JobSpecification: &jobSpecification,
	}, nil, nil, nil, nil)

	if err != nil {
		log.Fatal(err)
	}

	taskClient := batchARM.NewTaskClient()

	workerContainerSettings := batchARM.TaskContainerSettings{
		ImageName: &workerImage,
	}

	env1Key, env1Value := "1321", "312321"
	containerCommandLine := "echo Starting container..."

	workerEnvSettings := []batchARM.EnvironmentSetting{
		batchARM.EnvironmentSetting{Name: &env1Key, Value: &env1Value},
	}

	autoUser := batchARM.AutoUserSpecification{
		ElevationLevel: batchARM.Admin,
		Scope:          batchARM.Task,
	}

	tasks := make([]batchARM.TaskAddParameter, completions)
	for index := 0; index < completions; index++ {
		taskDisplayName := fmt.Sprintf("task-%d", index)
		task := batchARM.TaskAddParameter{
			DisplayName:         &taskDisplayName,
			ContainerSettings:   &workerContainerSettings,
			EnvironmentSettings: &workerEnvSettings,
			CommandLine:         &containerCommandLine,
			UserIdentity: &batchARM.UserIdentity{
				AutoUser: &autoUser,
			},
		}
		tasks[index] = task
	}

	taskCollection := batchARM.TaskAddCollectionParameter{
		Value: &tasks,
	}

	// jobAddParameter := batchARM.JobAddParameter{
	// 	DisplayName:          &jobName,
	// 	OnAllTasksComplete:   batchARM.TerminateJob,
	// 	UsesTaskDependencies: &TRUE,
	// 	PoolInfo: &batchARM.PoolInformation{
	// 		PoolID: &poolID,
	// 	},
	// }

	_, err = taskClient.AddCollection(ctx, jobID, taskCollection, nil, nil, nil, nil)
	if err != nil {
		// TODO: close the job?
		log.Fatal(err)
	}
}
