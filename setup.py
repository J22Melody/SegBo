from setuptools import setup


setup(
    name='cldfbench_SegBo',
    py_modules=['cldfbench_segbo'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'segbo=cldfbench_segbo:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
        'pyglottolog',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
