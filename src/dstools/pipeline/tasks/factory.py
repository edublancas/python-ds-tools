import inspect


class TaskFactory:
    """Utility class for reducing boilerplate code
    """

    def __init__(self, task_class, product_class, dag):
        self.task_class = task_class
        self.product_class = product_class
        self.dag = dag

    def make(self, task_arg, product_arg, params=None):
        # FIXME: only works for python callables
        # maybe each task class should implement a get_name method?
        name = inspect.getmodule(task_arg).__name__

        return self.task_class(task_arg,
                               product=self.product_class(product_arg),
                               dag=self.dag,
                               name=name,
                               params=params)
