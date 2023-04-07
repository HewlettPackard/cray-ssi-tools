# collect.py

Python 3 based utility to gather key artifacts from a Cray EX (CSM) cluster,
useful for archiving, investigating problems, and preparing for an upgrade
to a newer version.

## Usage

    % python3 collect.py --help
    usage: collect.py [-h] [-b BRANCH] [-c] [-e] [-g] [-i] [-o OUTPUT] [-s] [-f SWITCH_CONFIG] target_dir

    Collect important details of a Shasta cluister

    positional arguments:
    target_dir            Directory to store the important details of a cluster

    options:
    -h, --help            show this help message and exit
    -b BRANCH, --branch BRANCH
                            Default branch to checkout in the git repo backup
    -c, --clean           Optionally clean up target_directory first
    -e, --certs           Save certificates used to generate sealsed-secrets in customizations.yaml
    -g, --golden          Create a 'golden configuration' state of a cluster
    -i, --images          Store IMS image artifacts
    -o OUTPUT, --output OUTPUT
                            Store the collected artifacts to a file instead of the target_dir
    -s, --skip            Skip long running collection steps
    -f SWITCH_CONFIG, --switch_config SWITCH_CONFIG
                            Optional switch configuration file for GOSS tests

## Dependencies

Install the dependencies using the command

    pip install -U pip
    pip install -r requirements.txt

## Golden Configuration

By default, the collect script will capture as much information about a
Shasta cluster that an Administrator would find useful to help recover from a
failure, or to ease the process of upgrading or fresh installing a cluster.
This may result in an excess of files that may or may not be used on the
system.

Using the '-g' option will enable capturing a Golden Configuration of a 
cluster, limiting the output to include only CFS configurations actually in 
use on the cluster, and GIT configuration repositories limited only to a branch
specified by the '-b' option (default: integration).

The golden configuration mode requires the '-o' option which will export
the output into a .gz, .bz2, or .xz compressed tar archive depending
on the extension specified (default: .gz). The resulting image will be
a) significantly smaller than the raw exported contents (~7.5MB vs. 1.7MB) 
due to this compression; and b) easier to store since it is a single file.

