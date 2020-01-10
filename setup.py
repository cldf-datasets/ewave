from setuptools import setup


setup(
    name='cldfbench_ewave',
    py_modules=['cldfbench_ewave'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'cldfbench.dataset': [
            'ewave=cldfbench_ewave:Dataset',
        ]
    },
    install_requires=[
        'pycldf>=1.10',
        'cldfbench',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
