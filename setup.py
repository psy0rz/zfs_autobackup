import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="zfs_autobackup", 
    version="3.0-beta3",
    author="Edwin Eefting",
    author_email="edwin@datux.nl",
    description="ZFS autobackup is used to periodicly backup ZFS filesystems to other locations. This is done using the very effcient zfs send and receive commands.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/psy0rz/zfs_autobackup",
    scripts=["bin/zfs_autobackup"],
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
