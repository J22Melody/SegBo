from setuptools import setup


setup(
    name='cldfbench_SegBo',
    py_modules=['cldfbench_SegBo'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'SegBo=cldfbench_SegBo:Dataset',
        ]
    },
    install_requires=[
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
