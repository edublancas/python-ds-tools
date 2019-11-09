from collections.abc import Mapping
import warnings


class ProductsContainer:

    def __init__(self, products):
        self.products = products

    def __iter__(self):
        if isinstance(self.products, Mapping):
            for product in self.products.values():
                yield product
        else:
            for product in self.products:
                yield product

    def __getitem__(self, key):
        return self.products[key]

    def _to_json_serializable(self):
        """Returns a JSON serializable version of this product
        """
        if isinstance(self.products, Mapping):
            return {name: str(product) for name, product
                    in self.products.items()}
        else:
            return list(str(product) for product in self.products)


class MetaProduct:
    """
    Exposes a Product-like API for a list of products, used internally
    when a Task is declared to have more than one product so they can be
    easily accesed via product[0] or product['name'] if initialized with a
    mapping object, it is also used in a DAG to expose
    a limited version of a Product API which is used when a DAG is declared
    as an upstream dependency of a Task
    """

    def __init__(self, products):
        self.products = ProductsContainer(products)

    @property
    def metadata(self):
        # this has to happen dynamically since it depends on
        # the tasks being rendered already
        return {p: p.metadata for p in self.products}

    @property
    def timestamp(self):
        timestamps = [p.timestamp
                      for p in self.products
                      if p.timestamp is not None]
        if timestamps:
            return max(timestamps)
        else:
            return None

    @property
    def stored_source_code(self):
        stored_source_code = set([p.stored_source_code
                                  for p in self.products
                                  if p.stored_source_code is not None])
        if len(stored_source_code):
            warnings.warn(f'Stored source codes for products {self.products} '
                          'are different, but they are part of the same '
                          'MetaProduct, returning stored_source_code as None')
            return None
        else:
            return list(stored_source_code)[0]

    @property
    def task(self):
        return self.products[0].task

    @property
    def name(self):
        return ', '.join([p.name for p in self.products])

    @task.setter
    def task(self, value):
        for p in self.products:
            p.task = value

    @timestamp.setter
    def timestamp(self, value):
        for p in self.products:
            p.metadata['timestamp'] = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        for p in self.products:
            p.metadata['stored_source_code'] = value

    def exists(self):
        return all([p.exists() for p in self.products])

    def delete(self, force=False):
        for product in self.products:
            product.delete(force)

    def _outdated(self):
        return (self._outdated_data_dependencies()
                or self._outdated_code_dependency())

    def _outdated_data_dependencies(self):
        return any([p._outdated_data_dependencies()
                    for p in self.products])

    def _outdated_code_dependency(self):
        return any([p._outdated_code_dependency()
                    for p in self.products])

    def _to_json_serializable(self):
        """Returns a JSON serializable version of this product
        """
        # NOTE: this is used in tasks where only JSON serializable parameters
        # are supported such as NotebookRunner that depends on papermill
        return self.products._to_json_serializable()

    def save_metadata(self):
        for p in self.products:
            p.save_metadata()

    def render(self, params, **kwargs):
        for p in self.products:
            p.render(params, **kwargs)

    def short_repr(self):
        return ', '.join([p._short_repr() for p in self.products])

    def __repr__(self):
        reprs = ', '.join([repr(p) for p in self.products])
        return f'{type(self).__name__}: {reprs}'

    def __str__(self):
        strs = ', '.join([str(p) for p in self.products])
        return f'{type(self).__name__}: {strs}'

    def __iter__(self):
        for product in self.products:
            yield product

    def __getitem__(self, key):
        return self.products[key]
