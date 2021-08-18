# Contributing to segbo


## Prerequisites

Clone the repos
```
git clone --recurse-submodules https://github.com/cldf-datasets/segbo
```

Install the python dependencies:
```
cd segbo
pip install -e .[test]
```

## Curation workflow

Update the raw data:
```
cldfbench download cldfbench_segbo.py
```

Recreate the CLDF dataset:
```
cldfbench makecldf cldfbench_segbo.py
```

Check validity of the CLDF data:
```
pytest
```

