import setuptools
from zfs_autobackup.ZfsAutobackup import ZfsAutobackup
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="zfs_autobackup",
    version=ZfsAutobackup.VERSION,
    author="Edwin Eefting",
    author_email="edwin@datux.nl",
    description="ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. It tries to be the most friendly to use and easy to debug ZFS backup tool.",
    long_description=long_description,
    long_description_content_type="text/markdown",

    url="https://github.com/psy0rz/zfs_autobackup",
    entry_points={
        'console_scripts':
            [
                'zfs-autobackup = zfs_autobackup:cli',
            ]
    },
    packages=setuptools.find_packages(),

    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
    install_requires=[
        "colorama",
        "argparse"
    ]
)
