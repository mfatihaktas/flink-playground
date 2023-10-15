# flink-playground

Contains code to experiment with `pyflink`.

## Notes

* The directory in which `pyflink` writes the logs can be found as
```bash
python -c "import pyflink;import os;print(os.path.dirname(os.path.abspath(pyflink.__file__))+'/log')"
```
