package com.coralmesh.temporal;

import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.client.WorkflowClient;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
public class HelloWorker {
    public static void main(String[] args) {
        // This gRPC stubs wrapper talks to the local docker instance of the Temporal service.
        WorkflowServiceStubs service = WorkflowServiceStubs.newLocalServiceStubs();
        WorkflowClient client = WorkflowClient.newInstance(service);
        // Create a Worker factory that can be used to create Workers that poll specific Task Queues.
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker helloWorker = factory.newWorker(Shared.HELLO_WORLD_TASK_QUEUE);
        // This Worker hosts both Workflow and Activity implementations.
        // Workflows are stateful, so you need to supply a type to create instances.
        helloWorker.registerWorkflowImplementationTypes(HelloWorkflowImpl.class);
        // Activities are stateless and thread safe, so a shared instance is used.
        helloWorker.registerActivitiesImplementations(new FormatImpl());
        // Start polling the Task Queue.
        factory.start();
    }
}
