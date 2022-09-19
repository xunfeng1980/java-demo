package com.coralmesh.automatiko;

import io.automatiko.engine.api.Workflows;
import io.automatiko.engine.workflow.builder.WorkflowBuilder;

@Workflows
public class MyWorkflows {

    public WorkflowBuilder helloWorld() {

        WorkflowBuilder builder = WorkflowBuilder.newWorkflow("hello", "Sample Hello World workflow", "1")
                .dataObject("name", String.class);

        builder
                .start("start here").then()
                .log("say hello", "Hello world").then()
                .end("done");

        return builder;
    }
}