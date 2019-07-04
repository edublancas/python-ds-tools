# notes on render

* currently Task.render is only called by the DAG, to make sure tasks are rendered in the right order
* it works so far since we only need rendering when running the dag or plotting it, both actions happen at the dag level
* but this has an inconsistency at the task level, doing task.params returns different results pre and post rendering since task.params are modified by the rendering step, this inconsistency propagates to all methods that use params: build, plan and possibly status in the future
* one way to fix this is to always run render before this methods are run, but we cannot render tasks in isolation, the current task.render method just renders the current task, so if any
templates are present, this method will raise an error,
to fix this we will need to run all upstream dependencies before (similar to what it is done at the dag level)
* but implementing this logic means that we are rendering reduntantly since calling task.render at the DAG level will render all tasks a lot of times(!), while rendering is fast, it doesnt make sense for task.render to render all its upstream dependencies and itself
* i think the best solution is just to either show a warning or an exception if the task has not been rendered and it needs to