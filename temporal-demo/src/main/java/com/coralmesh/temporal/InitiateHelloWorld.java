package com.coralmesh.temporal;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;

import java.time.Duration;

public class InitiateHelloWorld {

    public static void main(String[] args) throws Exception {
        // This gRPC stubs wrapper talks to the local docker instance of the Temporal service.
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        // WorkflowClient can be used to start, signal, query, cancel, and terminate Workflows.
        WorkflowClient client = WorkflowClient.newInstance(service);
        WorkflowOptions options = WorkflowOptions.newBuilder()
                .setWorkflowTaskTimeout(Duration.ofMillis(60))
                .setTaskQueue(Shared.HELLO_WORLD_TASK_QUEUE)
                .build();
        // WorkflowStubs enable calls to methods as if the Workflow object is local, but actually perform an RPC.
        HelloWorkflow helloWorkflow = client.newWorkflowStub(HelloWorkflow.class, options);
        // Synchronously execute the Workflow and wait for the response.
        String greeting = helloWorkflow.getGreeting("World");
        System.out.println(greeting);
        System.exit(0);
    }
}
