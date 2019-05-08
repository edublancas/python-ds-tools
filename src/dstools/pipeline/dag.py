import subprocess
import tempfile
import networkx as nx
# https://networkx.github.io/documentation/networkx-1.10/reference/drawing.html
# # http://graphviz.org/doc/info/attrs.html
# NOTE: requires pygraphviz and pygraphviz


class DAGProduct:
    """
    A class that exposes a Product-like API for representing
    a DAG status
    """

    def __init__(self, dag):
        self.dag = dag

    @property
    def identifier(self):
        return [t.product.identifier for t in self.dag.tasks]

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def outdated_data_dependencies(self):
        return any([t.product.outdated_data_dependencies()
                    for t in self.dag.tasks])

    def outdated_code_dependency(self):
        return any([t.product.outdated_code_dependency()
                    for t in self.dag.tasks])

    @property
    def timestamp(self):
        timestamps = [t.product.timestamp
                      for t in self.dag.tasks
                      if t.product.timestamp is not None]
        if timestamps:
            return max(timestamps)
        else:
            return None


class DAG:

    def __init__(self, name=None):
        self.tasks = []
        self.tasks_by_name = {}
        self.product = DAGProduct(self)
        self.name = name

    def add_task(self, task):
        self.tasks.append(task)

        if task.name is not None:
            self.tasks_by_name[task.name] = task

    def mk_graph(self, add_properties=False):
        """
        Return a networkx directed graph from declared tasks and declared
        upstream dependencies
        """
        G = nx.DiGraph()

        for t in self.tasks:
            G.add_node(t)
            G.add_edges_from([(up, t) for up in t.upstream])

        if add_properties:
            for n, data in G.nodes(data=True):
                data['color'] = 'red' if n.product.outdated() else 'green'
                data['label'] = n.short_repr()

        return G

    def render(self):
        G = self.mk_graph(add_properties=False)

        for t in nx.algorithms.topological_sort(G):
            t.render()

    def build(self):
        # FIXME: must render first
        # attributes docs:
        # https://graphviz.gitlab.io/_pages/doc/info/attrs.html
        G = self.mk_graph(add_properties=True)

        for t in nx.algorithms.topological_sort(G):
            t.build()

    def plot(self):
        # FIXME: must render first
        G = self.mk_graph(add_properties=True)
        G_ = nx.nx_agraph.to_agraph(G)
        path = tempfile.mktemp(suffix='.png')
        G_.draw(path, prog='dot', args='-Grankdir=LR')
        subprocess.run(['open', path])

    # def __getitem__(self, key):
        # return self.tasks_by_name[key]

    def to_dict(self):
        g = self.mk_graph()
        return {n.name: n for n in g.nodes()}

    def __repr__(self):
        name = self.name if self.name is not None else 'Unnamed'
        return f'{type(self).__name__}: {name}'

    def short_repr(self):
        return repr(self)
