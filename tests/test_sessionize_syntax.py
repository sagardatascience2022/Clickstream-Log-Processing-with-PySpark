import py_compile
import pathlib


def test_syntax():
    src = pathlib.Path(__file__).parents[1] / 'src' / 'sessionize.py'
    py_compile.compile(str(src), doraise=True)
