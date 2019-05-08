
"""
Products are persistent changes triggered by Tasks such as a new file
in the local filesystem or a table in a database
"""

import warnings
from pathlib import Path
from dstools.pipeline.identifiers import StringIdentifier


class Product:
    """A product is a persistent triggered by a Task
    """

    def __init__(self, identifier):
        self._identifier = StringIdentifier(identifier)
        self.tests, self.checks = [], []
        self.did_download_metadata = False
        self.task = None

    @property
    def identifier(self):
        return self._identifier()

    @property
    def timestamp(self):
        return self.metadata.get('timestamp')

    @property
    def stored_source_code(self):
        return self.metadata.get('stored_source_code')

    @property
    def task(self):
        if self._task is None:
            raise ValueError('This product has not been assigned to any Task')

        return self._task

    @property
    def metadata(self):
        if self.did_download_metadata:
            return self._metadata
        else:
            self.get_metadata()
            self.did_download_metadata = True
            return self._metadata

    @task.setter
    def task(self, value):
        self._task = value

    @timestamp.setter
    def timestamp(self, value):
        self.metadata['timestamp'] = value

    @stored_source_code.setter
    def stored_source_code(self, value):
        self.metadata['stored_source_code'] = value

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def outdated_data_dependencies(self):
        def is_outdated(up_prod):
            """
            A task becomes data outdated if an upstream product has a higher
            timestamp or if an upstream product is outdated
            """
            if self.timestamp is None or up_prod.timestamp is None:
                return True
            else:
                return ((up_prod.timestamp > self.timestamp)
                        or up_prod.outdated())

        outdated = any([is_outdated(up.product) for up in self.task.upstream])

        return outdated

    def outdated_code_dependency(self):
        return self.stored_source_code != self.task.source_code

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def get_metadata(self):
        """
        This method calls Product.fetch_metadata() (provided by subclasses),
        if some conditions are met, then it saves it in Product.metadata
        """
        metadata_empty = dict(timestamp=None, stored_source_code=None)
        # if the product does not exist, return a metadata
        # with None in the values
        if not self.exists():
            self.metadata = metadata_empty
        else:
            metadata = self.fetch_metadata()

            if metadata is None:
                self.metadata = metadata_empty
            else:
                # FIXME: we need to further validate this, need to check
                # that this is an instance of mapping, if yes, then
                # check keys [timestamp, stored_source_code], check
                # types and fill with None if any of the keys is missing
                self.metadata = metadata

    def test(self):
        """Run tests, raise exceptions if any of these are not true
        """
        for fn in self.tests:
            res = fn(self)
            if not res:
                raise AssertionError(f'{self} failed test: {fn}')

    def check(self):
        """
        Run checks, this are just for diagnostic purposes, if a any returns
        False, a warning is sent
        """
        for fn in self.checks:
            if not fn(self):
                warnings.warn(f'Check did not pass: {fn}')

    def pre_save_metadata_hook(self):
        pass

    def fetch_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def save_metadata(self):
        raise NotImplementedError('You have to implement this method')

    def exists(self):
        """
        This method returns True if the product exists, it is not part
        of the metadata, so there is no cached status
        """
        raise NotImplementedError('You have to implement this method')

    def render(self, params):
        """
        Render Product - this will render contents of Templates used as
        identifier for this Product, if a regular string was passed, this
        method has no effect
        """
        self._identifier.render(params)

    def __str__(self):
        return str(self.identifier)

    def __repr__(self):
        return f'{type(self).__name__}: {self.identifier}'

    def short_repr(self):
        return f'{self.identifier}'


class MetaProduct:
    """Exposes a Product-like API for a list of products
    """

    def __init__(self, products):
        self.products = products
        self.metadata = {p: p.metadata for p in self.products}

    @property
    def identifier(self):
        return [p.identifier for p in self.products]

    def exists(self):
        return all([p.exists() for p in self.products])

    def outdated(self):
        return (self.outdated_data_dependencies()
                or self.outdated_code_dependency())

    def outdated_data_dependencies(self):
        return any([p.outdated_data_dependencies()
                    for p in self.products])

    def outdated_code_dependency(self):
        return any([p.outdated_code_dependency()
                    for p in self.products])

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

    def save_metadata(self):
        for p in self.products:
            p.save_metadata()

    def render(self, params):
        for p in self.products:
            p.render(params)

    def pre_save_metadata_hook(self):
        pass

    def short_repr(self):
        return ', '.join([p.short_repr() for p in self.products])

    def __repr__(self):
        reprs = ', '.join([repr(p) for p in self.products])
        return f'{type(self).__name__}: {reprs}'


class File(Product):
    """A product representing a local file
    """
    @property
    def path_to_file(self):
        return Path(self.identifier)

    @property
    def path_to_stored_source_code(self):
        return Path(str(self.path_to_file) + '.source')

    def fetch_metadata(self):
        # we can safely do this here since this is only run when the file
        # exists
        timestamp = self.path_to_file.stat().st_mtime

        # but we have no control over the stored code, it might be missing
        # so we check
        if self.path_to_stored_source_code.exists():
            stored_source_code = self.path_to_stored_source_code.read_text()
        else:
            stored_source_code = None

        return dict(timestamp=timestamp, stored_source_code=stored_source_code)

    def save_metadata(self):
        # timestamp automatically updates when the file is saved...
        self.path_to_stored_source_code.write_text(self.stored_source_code)

    def exists(self):
        return self.path_to_file.exists()

    def __repr__(self):
        return f'File({repr(self._identifier)})'
