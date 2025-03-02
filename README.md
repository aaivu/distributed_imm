# Distributed IMM

**Scalable Iterative Mistake Minimization (IMM) for Clustering Explanations**

Distributed IMM is a scalable **PySpark** implementation of the IMM algorithm for clustering explanations. It includes **Cython-optimized histogram-based splitting** and **K-Means initialization** for efficiency.

## Features
- **Distributed IMM computation** for large datasets
- **Optimized histogram-based splitting** (Cython)
- **K-Means initialization** for clustering
- **Customizable verbosity** for debugging
- **Decision tree visualization** (Graphviz)

## Installation

### From PyPI (When Hosted)
```sh
pip install distributed_imm
```

### Local Installation (Development Mode)
```sh
git clone https://github.com/yourusername/distributed_imm.git
cd distributed_imm
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Development Guide
1. Create a feature branch: `git checkout -b feature-branch`
2. Develop and test locally
3. Push changes: `git push origin feature-branch`
4. Open a pull request & merge into `main`

## Building & Testing
```sh
python setup.py build_ext --inplace  # Build Cython
python setup.py sdist bdist_wheel  # Build package
pip install dist/distributed_imm-0.1.0.whl  # Install locally
```

## Uploading to PyPI
### Configure PyPI Credentials (`~/.pypirc`)
```
[pypi]
username = __token__
password = pypi-YourRealTokenHere
```

### Upload Steps
```sh
pip install --upgrade setuptools wheel twine
twine upload dist/*
```

## Versioning & Updates
- `0.1.x`: Bug fixes
- `0.2.x`: New features
- `1.x.x`: Major changes

Update `setup.py` and rebuild:
```sh
rm -rf dist/
python setup.py sdist bdist_wheel
twine upload dist/*
```


## License
MIT License



