import os
from setuptools import setup

setup(
    name='ducksauce',
    packages=["ducksauce"],
    version='1.0',
    entry_points={
        'console_scripts': [
            'ducksauce = ducksauce.__main__'
        ],
    },
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description="Fast enough sort of Apache Arrow streams",
    url="http://github.com/bmschmidt/ducksauce",
    author="Benjamin Schmidt",
    author_email="bmschmidt@gmail.com",
    license="MIT",
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        "Operating System :: Unix",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    install_requires=["pyarrow"]
)
