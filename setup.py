import setuptools
import bin.zfs_autobackup
import os

os.system("git tag -m ' ' -a v{}".format(bin.zfs_autobackup.VERSION))

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="zfs_autobackup", 
    version=bin.zfs_autobackup.VERSION,
    author="Edwin Eefting",
    author_email="edwin@datux.nl",
    description="ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. It tries to be the most friendly to use and easy to debug ZFS backup tool.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    
    url="https://github.com/psy0rz/zfs_autobackup",
    scripts=["bin/zfs-autobackup"],
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
