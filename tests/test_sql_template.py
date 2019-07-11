import pytest
from dstools.templates import SQLTemplate

t = SQLTemplate("""

SELECT * FROM {{table}}

""")


def test_raises_error_if_missing_parameter():
    with pytest.raises(TypeError):
        t.render()


def test_raises_error_if_extra_parameter():
    with pytest.raises(TypeError):
        t.render(not_a_param=1)