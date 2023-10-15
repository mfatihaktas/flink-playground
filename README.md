# flink-playground

Contains code to experiment with `pyflink`.

## Dependencies

* `direnv`
* `make`

## Notes

* Can find example commands in `run_tests.sh` to run the tests.

* The directory in which `pyflink` writes the logs can be found as
```bash
python -c "import pyflink;import os;print(os.path.dirname(os.path.abspath(pyflink.__file__))+'/log')"
```
