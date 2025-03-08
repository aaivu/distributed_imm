from setuptools import setup, find_packages, Extension
from Cython.Build import cythonize
import numpy

setup(
    name="distributed_imm",
    version="0.4.0",
    description="A distributed implementation of Iterative Mistake Minimization (IMM) for clustering explanations",
    author="Saadha",
    author_email="marium.20@cse.mrt.ac.lk",
    url="https://github.com/ScalableXplain/distributed_imm",
    packages=find_packages(),
    ext_modules=cythonize([
        Extension(
            "d_imm.splitters.cut_finder",
            ["d_imm/splitters/cut_finder.pyx"],
            extra_compile_args=['-fopenmp'],  # Enable OpenMP for parallel processing
            extra_link_args=['-fopenmp'],
        ),
    ]),
    include_dirs=[numpy.get_include()],
    install_requires=[
        "numpy",
        "pyspark",
        "cython",
        "graphviz",
        "pandas"
    ],
    license="MIT",  # Explicitly mention the license
    license_files=["LICENSE"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    zip_safe=False,
)
