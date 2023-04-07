#!/usr/bin/env python3
"""Collect important details of a Shasta cluster for use before and after installation and upgrades.

(C) Copyright 2022-2023 Hewlett Packard Enterprise Development LP

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
"""
import argparse
import base64
import json
import logging
import os
import pathlib
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib3
import yaml

from subprocess import PIPE
from sys import stderr, stdout
from typing import IO

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
fh = logging.StreamHandler()
fh_formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
fh.setFormatter(fh_formatter)
logger.addHandler(fh)

branch = None
clean_flag = False
quiet_flag = False
skip_flag = False
debug_flag = False
golden_flag = False
image_flag = False
cert_flag = False
name_field = "name"
switch_config = None

_json_cache = {}

cray_cmd = "/usr/bin/cray"
kubectl_cmd = "/usr/bin/kubectl"
ssh_cmd = "/usr/bin/ssh"
git_cmd = "/usr/bin/git"
tar_cmd = "/usr/bin/tar"

git_host = "api-gw-service-nmn.local"
git_user = "crayvcs"
tmp_repo_dir_name = "collect."
tmp_repo_tar_name = "collect."

class CollectArchiveException(Exception):
    pass

class CollectGITException(Exception):
    pass
        
def prep_target(target_dir):
    """Prepare a directory to store all the collected artifacts

    Args:
        target_dir (str): Directory path to store collected artifacts
    """
    if os.path.exists(target_dir):
        if clean_flag:
            try:
                logger.info(f"Removing {target_dir} due to clean_flag={clean_flag}")
                shutil.rmtree(target_dir)
            except:
                logger.error(f"Error deleting directory {target_dir}")
                exit(2)

    if os.path.exists(target_dir):
        if not os.path.isdir(target_dir):
            logger.error(f"Error: {target_dir} is not a directory")
            exit(1)
    else:
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True) 

def get_sw_admin_password(config_file):
    """Determine the SW_ADMIN_PASSWORD value.  If it is not set in the calling environment,
    try to load if from a config file.

    Args:
        config_file (src): Configuration file to try to load the password value from if not set in the environment
    """
    if "SW_ADMIN_PASSWORD" not in os.environ:
        if switch_config is not None and os.path.exists(config_file) and os.path.isfile(config_file):
            with open(config_file) as fp:
                for line in fp.readlines():
                    line = line.strip()    
                    try:                    
                        var, value = line.split('=')
                        if var == "SW_ADMIN_PASSWORD":
                            os.environ["SW_ADMIN_PASSWORD"] = value
                            return
                    except ValueError as e:
                        # print(str(e))
                        pass
        else:
            # config does not exist, or is not a file, do nothing   
            pass
    else:
        # SW_ADMIN_PASSWORD is already set, allow that to override the config file
        pass

def run_health_checks(target_dir):
    """Run and capture the output of the health scripts

    Args:
        target_dir (String): Directory to store the output and error files from the health scripts
    """
    ret = False

    if not os.path.exists(target_dir):
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True) 

    cmds = [
        "/opt/cray/platform-utils/ncnHealthChecks.sh", 
        "/opt/cray/platform-utils/ncnPostgresHealthChecks.sh"
    ]

    get_sw_admin_password(switch_config)
    if "SW_ADMIN_PASSWORD" in os.environ:
        # Add the GOSS tests if they exist.  There are some circumstances when
        # the individual tests have to be run, but optimally the combined check
        # will exist and be used.
        automated_tests = "/opt/cray/tests/install/ncn/automated"
        if os.path.exists(automated_tests):
            combined_tests = os.path.join(automated_tests, "ncn-k8s-combined-healthcheck")
            if os.path.exists(combined_tests):
                cmds.insert(0, combined_tests)
            else:
                individual_automated_tests = [
                    "ncn-healthcheck",
                    "ncn-kubernetes-checks", 
                    "ncn-postgres-tests"
                ]
                for single_test in individual_automated_tests:
                    test_path = os.path.join(automated_tests, single_test)
                    if os.path.exists(test_path):
                        cmds.insert(0, test_path)
        else:
            # GOSS tests don't exist, skip trying to run them
            pass
    else:
        # Switch password is not set in the environment, nor config file, so skip the GOSS tests
        pass
    
    for cmd in cmds:
        script_name = os.path.basename(cmd)
        std_output = os.path.join(target_dir, script_name + ".out")

        with open(std_output, "wb") as out:
            logger.info(f"Capturing output of {cmd} to {std_output}")
            p = subprocess.Popen(cmd.split(), stdout=out, stderr=out, cwd=target_dir, shell=False)
            rc = p.wait()

def get_components_desired_configs(only_enabled=False):
    """Return a list of configurations assigned (desired) by components

    Args:
        only_enabled (bool, optional): Include only enabled components/configurations. Defaults to False.

    Returns:
        list: List of applicable desired configs in use on the cluster
    """
    ret = []
    cmd = f"{cray_cmd} cfs components list --format json"
    p = subprocess.Popen(cmd.split(), shell=False, stdout=PIPE, stderr=PIPE)

    stdout, stderr = p.communicate()
    try:
        data = json.loads(stdout.decode())
        for entry in data:
            enabled = None

            if "id" in entry:
                id = entry["id"]
            
                if "enabled" in entry:
                    enabled = entry["enabled"]
                
                if "desiredConfig" in entry:
                    desired_config = entry["desiredConfig"]

                if only_enabled and not enabled:
                    continue
                else:
                    if desired_config not in ret:
                        ret.append(desired_config)
    except json.decoder.JSONDecodeError:
        logger.error(f"Error: failed to decode JSON data from the output of [{cmd}]")

    rc = p.wait() # make sure the command completes, but after the possible buffering of stdout

    return ret

def get_active_cfs_configurations():
    """Return a list of CFS configurations that have been assigned to one or more 
    component in CFS

    Returns:
        list: Sorted list of unique CFS configurations assigned to one or more components
    """
    cmd = f"{cray_cmd} cfs components list --format json"
    found_components = get_json_output(cmd)
    active_configs = []
    for config in found_components:
        try:
            name = config["desiredConfig"]
            id = config["id"]
            if name == "":
                logger.warning(f"Found a CFS configuration with an empty name assigned to {id}")
            else:
                if name not in active_configs:
                    active_configs.append(name)
                    logger.debug(f"Adding {name} to list of active CFS configurations")
                else:
                    # logger.debug(f"{name} already included in the list of active CFS configurations")
                    pass
        except KeyError:
            logger.debug(f"CFS configuration did not include a desiredConfig value: {config}")
            pass

    active_configs.append("ncn-image-customization")
    return sorted(active_configs)

def save_cfs_configurations(target_dir):
    """Collect CFS configurations, both the existing raw configuration, and a version
    with the name and lastModified fields striped out, appropriate for re-importing.

    The std_output file name is based the "name" field being present in the returned
    JSON data.

    """
    active_configs = None
    if golden_flag:
        logger.debug("Golden flag is set, limited CFS configurations to only those currently assigned to a component")
        active_configs = get_active_cfs_configurations()
        logger.debug("Golden CFS configurations: " + ", ".join(active_configs))
    cmd = f"{cray_cmd} cfs configurations list --format json"
    save_json_config(cmd, target_dir, active_configs)

def save_bos_sessiontemplates(target_dir):
    """Collect CFS configurations, both the existing raw configuration, and a version
    with the name and lastModified fields striped out, appropriate for re-importing.

    The std_output file name is based the "name" field being present in the returned
    JSON data.

    """
    cmd = f"{cray_cmd} bos v1 sessiontemplate list --format json"
    save_json_config(cmd, target_dir)

def save_sls_dumpstate(target_dir):
    """Collect SLS dumpstate configuration.

    Args:
        target_dir (String): Directory to store the resulting JSON configuration file
    """
    cmd = f"{cray_cmd} sls dumpstate list --format json"

    if not os.path.exists(target_dir):
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True) 

    sls_dumpstate = get_json_output(cmd)
    sls_dumpstate_file = os.path.join(target_dir, "sls_dumpstate.json")
    with open(sls_dumpstate_file, 'w') as fp:
        logger.info(f"Exporting SLS to {sls_dumpstate_file}")
        fp.write(json.dumps(sls_dumpstate, indent=2))

def save_bootprep_files(target_dir):
    """Collect SAT bootprep files from the standard SSI location on disk.  This will only backup files
    named with a .yml or .yaml extension, and is greater than zero bytes.  If identically named files 
    are found (eg. same filenames, in different directories), subsequent copies will append a unique
    string to the end.  No YaML validation is performed so the collect script will not require the extra
    package dependency that would be incurred.
    """
    bootprep_location = "/mnt/admin/configs/bootprep"
    skip_filenames = [
        "cabinets.yaml",
        "application_node_config.yaml",
        "customizations.yaml",
        "custom_switch_config.yaml",
        "metallb.yaml",
        "product_vars.yaml",
        "system_config.yaml"
    ]  # these really should not be in the bootprep directory, but sometimes they are.

    if not os.path.exists(target_dir):
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    if os.path.exists(bootprep_location) and os.path.isdir(bootprep_location):
        found_count = 0
        for dirpath, dirname, filenames in os.walk(bootprep_location):
            for fname in filenames:
                if fname not in skip_filenames:
                    tname = os.path.join(dirpath, fname)
                    if tname.endswith("yaml") or tname.endswith("yml"):
                        if os.path.getsize(tname) > 0:
                            target_file = os.path.join(target_dir, fname)
                            if os.path.exists(target_file):
                                temp_name = next(tempfile._get_candidate_names())
                                target_file += f".{temp_name}"
                                logger.warning(f"{target_file} collision detected, renaming destination file to {target_file}")
                            shutil.copyfile(tname, target_file)
                            logger.debug(f"Copied {tname} to {target_file}")
                            found_count += 1
                        else:
                            logger.warning(f"Skipping {tname} since it is empty")
        logger.info(f"Copied {found_count} bootprep files from {bootprep_location} to {target_dir}")
                    
    else:
        logger.warning(f"Warning: {bootprep_location} does not exist, skipping save_bootprep_files()")

def get_json_output(cmd=None, limit=None):
    """Obtain and validate JSON output of a command, from a cache, or the live execution of that command.
    Warning: the cache mechanism used here is not threadsafe per se.

    Args:
        cmd (String): Command line to execute
        limit (_type_, optional): _description_. Defaults to None.
    """
    data = {}
    global _json_cache

    if cmd is not None:
        if not cmd.endswith("--format json") and not cmd.endswith("-o json"):
            cmd += " --format json"
            logger.debug(f"Modified command to include format parameter: {cmd}")

        if cmd not in _json_cache:       
            logger.debug(f"Cache MISS for {cmd}") 

            p = subprocess.Popen(cmd.split(), shell=False, stdout=PIPE, stderr=PIPE)
            stdout, stderr = p.communicate()
            try:
                data = json.loads(stdout.decode())
                _json_cache[cmd] = data # stash output into global cache
            except json.JSONDecodeError as e:
                logger.warning(f"Warning: encountered an error decoding JSON about from command ({cmd})")
                logger.debug(f"Error was: {e}")
        else:
            logger.debug(f"Cache HIT for {cmd}")
            data = _json_cache[cmd]
    return data

def save_json_config(cmd, target_dir, limit=None):
    """Save filtered JSON output to a file.  This uses the get_json_output() method which will 
    return cached data if it has already been obtained once.

    Args:
        cmd (String): Command line to execute
        target_dir (String): Path to save the JSON output to
        limit ([type], optional): [description]. Defaults to None.
    """   
    if not os.path.exists(target_dir):
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True) 

    if limit is not None and type(limit) == list:
        logger.debug(f"Limiting JSON configurations to {limit}")

    data = get_json_output(cmd)
    for entry in data:
        if "name" in entry:
            config_name = entry[name_field]
            # sanitize the name field per SSI-2251
            config_name = "".join(i for i in config_name if i not in r'\ \/~:*?"<>|')
            config_fname = os.path.join(target_dir, f"{config_name}.json")

            if limit is not None and type(limit) == list:
                if not config_name in limit:
                    # logger.debug(f"Skipping {config_name} since it is not in {limit}")
                    continue

            if name_field in entry:
                entry.pop(name_field)
            if "lastUpdated" in entry:
                entry.pop("lastUpdated")

            with open(config_fname, "w") as fp:
                logger.info(f"Exporting {config_name} to {config_fname}")
                fp.write(json.dumps(entry, indent=2))
    
def get_slingshot_fm_pod():
    """Determine the name of the Slingshot Fabric Manager k8s POD.  This will return 
    the first name found.

    Returns:
        string: Name string name of the Slingshot Fabric Manager POD
    """
    fm_re = re.compile("^(?P<pod>slingshot-fabric-manager-[a-z0-9]+-[a-z0-9]+)\s+.+", re.IGNORECASE)
    ret = None
    cmd = f"{kubectl_cmd} -n services get pods"
    p = subprocess.Popen(cmd.split(), shell=False, stdout=PIPE, stderr=PIPE)
    rc = p.wait()

    if rc == 0:
        stdout, stderr = p.communicate()
        for line in str(stdout).split('\\n'):
            fm_mo = fm_re.match(line)
            if fm_mo:
                ret = fm_mo.group("pod")

    return ret
        
def save_sshot_files(target_dir, files):
    """Save a list of files from the slingshot fabric manager pod

    Args:
        target_dir (string): Directory to store the configuration files into
        files (array): List of files to backup from the POD
    """
    pod = get_slingshot_fm_pod()

    if not os.path.exists(target_dir):
        pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True) 

    for f in files:
        fname = os.path.basename(f)
        target_fname = os.path.join(target_dir, fname)
        cmd = f"{kubectl_cmd} -n services cp {pod}:{f} {target_fname}"
        p = subprocess.Popen(cmd.split(), stdout=PIPE, stderr=PIPE, shell=False)
        rc = p.wait()

        if rc == 0:
            logger.info(f"Copied Slingshot Config {f} to {target_fname}")
        else:
            logger.error(f"Error: Failed to copy Slingshot Config {f} to {target_fname}")

def run_sshot_backup(target_dir):
    """Run the fmn-backup command in the slingshot pod, copy the backup file and the
    p2p and fabric template files.

    Args:
        target_dir (string): Directory to save the backup files into
    """
    # files will be appended with whatever fmn-backup returns
    files = [
        "/opt/cray/etc/sct/Shasta_system_hsn_pt_pt.csv",
        "/opt/cray/fabric_template.json"
    ]
    pod = get_slingshot_fm_pod()
    file_re = re.compile("^SUCCESS: .+ File:(?P<filename>.+$)", re.IGNORECASE)

    pod_cmd = f"{kubectl_cmd} -n services exec -it {pod} -- /usr/bin/fmn-backup"
    p = subprocess.Popen(pod_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    backup_success = False
    for line in out.decode().split("\n"):
        line = line.strip()
        file_mo = file_re.match(line)
        if file_mo:
            files.append(file_mo.group("filename"))
            backup_success = True
        else:
            pass
    
    rc = p.wait()
    if not backup_success:
        logger.warning("Warning: fmn-backup did not generate a backup file")

    save_sshot_files(target_dir, files)

def get_gitea_repo_list(username, password):
    """Generate a list of GIT repositories hosted in the Shasta gitea instance

    Args:
        username (str): Username for API authenticated requests
        password (str): Password for API authenticated requests

    Returns:
        list: List of GIT repositories in gitea
    """
    ret = []
    uri = "https://api-gw-service-nmn.local/vcs/api/v1/repos/search"
    conn = urllib3.PoolManager()
    headers = urllib3.make_headers(basic_auth=f"{username}:{password}")
    r = conn.request('GET', uri, headers=headers)
    if r.status == 200:
        try:
            repos = json.loads(r.data)
            for repo in repos["data"]:
                name = repo["name"]
                html_url = repo["html_url"]
                ssh_url = repo["ssh_url"]
                clone_url = repo["ssh_url"]
                ret.append((name, clone_url))
        except KeyError as e:
            logger.error(f"Expected data returned in the request to list gitea repositories is missing! ({e})")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode a JSON payload from {uri} request, will not be able to backup config repos")
    else:
        logger.error(f"Failed to list gitea repositories ({r.status_code})")

    return ret

def git_checkout_branch(target, branch_name):
    """Checkout a branch in a given local GIT repository

    Args:
        target (str): Path on disk to the git repository
        branch_name (str): Branch name the should be checked out
    """
    cmd = f"{git_cmd} checkout {branch_name}"
    p = subprocess.Popen(cmd.split(), cwd=target, stdout=PIPE, stderr=PIPE)
    rc = p.wait()

    if rc != 0:
        logger.warning(f"Warning: {target} did not include a branch named {branch_name}, default branch will be used")

def git_convert_bare_repo(target, branch_name):
    """Run the commands to convert a git repo from a bare repository to a working copy.

    Args:
        target (str): Path on disk to the git repository
        branch_name (str): Branch name that should be checked out

    Raises:
        CollectArchiveException: Raised to indicate a significant failure in converting the repository that may
        leave the directory incomplete.
    """
    cmd = f"{git_cmd} config --bool core.bare false"
    p = subprocess.Popen(cmd.split(), cwd=target, stdout=PIPE, stderr=PIPE)
    rc = p.wait()
    if rc != 0:
        raise CollectArchiveException(f"Failed to convert bare repository {target}, directory may be incomplete")
    
    git_checkout_branch(target, branch_name)

def git_reset(target):
    """Perform a git hard reset to return a repository working copy to the committed state.

    Args:
        target (str): Path on disk to the git repository

    Raises:
        CollectArchiveException: Raised to indicate a significant failure in reset'ing the git repository
        that may leave the directory incompete.
    """
    cmd = f"{git_cmd} reset --hard"
    p = subprocess.Popen(cmd.split(), cwd=target, stdout=PIPE, stderr=PIPE)
    rc = p.wait()
    if rc != 0:
        raise CollectArchiveException(f"Failed to reset git repository {target}, directory may be incomplete")

def clone_git_bare_repo(target, username, password, repo):
    """Expore a GIT repository has a bare clone, which will include the contents of all the 
    branches in the remote repo.
    """
    cmd = f"{git_cmd} clone --bare {repo} {target}/.git"
    p = subprocess.Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    rc = p.wait()

    if rc != 0:
        raise CollectGITException(f"Failed to create a bare cllone of {repo}")

    # Since the bare repo will now have all the branches, convert the resulting
    # directory into a standard repo that people are used to working with.
    git_convert_bare_repo(target, branch)
    git_reset(target)

def save_repos(target):
    """Save a copy of all the GIT configuraiton repositories that include one or more branches
    with "integration" in the name.  Most of the time there will be only one integration branch,
    but in the case of a system with one or more GPUs, there may be multiple integration branches
    for the cos compute or uan nodes.

    Args:
        target (string): Directory to store a copy of the repository directory to
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
    
    cleanup = []
    git_password = get_vcs_credentials()
    repos = get_gitea_repo_list(git_user, git_password)
    repo_count = len(repos)
    logger.info(f"There are {repo_count} repositories to backup")

    for repo_name, clone_url in repos:
        repo_dir = os.path.join(target, repo_name)
        pathlib.Path(repo_dir).mkdir(parents=True, exist_ok=True)
            
        kruft, git_path = clone_url.split(':')
        new_clone_url = f'https://crayvcs:{git_password}@api-gw-service-nmn.local/vcs/{git_path}'
        try:
            clone_git_bare_repo(repo_dir, git_user, git_password, new_clone_url)
            logger.info(f"Created repository clone {repo_name}")

            if golden_flag:
                git_dir = os.path.join(repo_dir, ".git")
                repo_dir_w_branch = f"{repo_dir}-{branch}"
                shutil.rmtree(git_dir) # remove GIT artifacts
                os.rename(repo_dir, repo_dir_w_branch)
                logger.info(f"Converted GIT repo {repo_dir} to {repo_dir_w_branch} to preserve only golden configuration contents")
        except CollectGITException as e:
            logger.error(f"Failed to create bare repository clone of {repo_name}: {e}")
        except OSError as e:
            if golden_flag:
                logger.error(f"Failed to export golden configuration limited version of {repo_dir} due to a file system error: {e}")
            else:
                logger.error(f"Failed to export a bare repository clone of {repo_dir} due to a file system error: {e}")

def gather_inventory(target):
    """Gather various inventory information to the specified target directory

    Args:
        target (string): Directory to store the inventory information to
    """
    inventory = gather_hardware_inventory(target)
    nic_inventory = gather_ethernet_inventory(target)
    generate_inventory_csv(inventory, nic_inventory, target)
    
def generate_inventory_csv(inventory, nic_inventory, target):
    """Generate an comma separated representation of the inventory data

    Args:
        inventory (dict): JSON dictionary of inventory data
        nic_inventory (dict): JSON dictionary of NIC inventory data
        target (str): Output directory to store the resulting CSV file into
    """
    interesting_types = ["Node", "NodeBMC"]
    output = os.path.join(target, "inventory.csv")

    try:    
        logger.info(f"Exporting processed inventory to {output}")

        with open(output, "w") as fp:
            headers = ",".join(nic_inventory[0].keys())
            fp.write(f"{headers}\n")
            for e in nic_inventory:
                entry = ""
                for v in e.values():
                    # The IP address column can appear as a list, grab the first one
                    # TODO: Check larger systems to verify that only one value is 
                    # returned.
                    if type(v) == list:
                        if len(v) > 0:
                            va = v[0]
                            if "IPAddress" in va:
                                va = va["IPAddress"]
                            # print(type(va))
                        else:
                            va = ""    
                    else:
                        va = v
                    entry += f"{va},"
                entry = entry[:-1]
                fp.write(f"{entry}\n")
    except KeyError as e:
        logger.warning("Warning: encountered an error generating CSV inventory data.  Some of the HSM inventory was probably not collected, check for previous errors.")
        logger.debug("Error was " + str(e))
        if os.path.exists(output):
            logger.debug(f"Removing {output} since it is (probably) empty")
            os.unlink(output)

def gather_hardware_inventory(target):
    """Gather the inentory from the HSN

    Args:
        target (string): Directory to store the inventory information

    Returns:
        list: JSON data representing the inventory data
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True) 
    output = os.path.join(target, "inventory.json")
    data = get_json_output("cray hsm inventory hardware list")
    
    with open(output, "w") as fp:
        logger.info(f"Exporting inventory to {output}")
        json.dump(data, fp, indent=2)

    return data

def gather_ethernet_inventory(target):
    """Gather NIC inventory from the HSN

    Args:
        target (string): Directory to store the inventory information

    Returns:
        list: JSON data representing the NIC inventory
    """
    output = os.path.join(target, "ethernet.json")

    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True) 

    data = {}
    try: 
        data = get_json_output("cray hsm inventory ethernetInterfaces list")
        with open(output, "w") as fp:
            logger.info(f"Exporting ethernet inventory to {output}")
            json.dump(data, fp, indent=2)
    except json.decoder.JSONDecodeError as e:
        logger.warning("Warning: encountered a problem decoding HSM NIC inventory data")
        logger.debug("Error was: " + str(e))
    return data

def get_product_catalog():
    """Return the cray product catalog

    Returns:
        [String]: [Return the JSON-formed cray product catalog data]
    """
    cmd = f"{kubectl_cmd} get cm -n services cray-product-catalog -o json"
    return get_json_output(cmd)

def get_csm_version():
    """Check the product catalog, and return the active version of CSM,
    or None (if CSM is not installed, or no version is set active). 

    Returns:
        string: Semantic version (1.4.0), occasionally includes build version info, 
        also (1.4.0-alpha.29) which does not necessarily abide a single standard.
    """
    try:
        yaml_product_data = yaml.safe_load(get_product_catalog()["data"]["csm"])
        for product_version in yaml_product_data:
            if yaml_product_data[product_version]["active"]:
                return product_version  # stop now, and return this version
    except KeyError as e:
        logger.exception(e)
    
    return None

def csm_compatibility_check():
    compatible_prefixes = ["1.2", "1.3", "1.4"]
    csm_version = get_csm_version()
    for prefix in compatible_prefixes:
        if csm_version.startswith(prefix):
            logger.debug(f'{csm_version} appears to be a compatible version with {prefix} version string')
            return True
        
    logger.warning(f'{csm_version} does not appear to be a compatible version ({compatible_prefixes})')
    return False

def get_ims_recipes():
    """Generate a JSON dictionary of IMS recipes

    Returns:
        dict: JSON dictionary of IMS recipes found on a Cray CSM cluster
    """
    cmd = f"{cray_cmd} ims recipes list --format json"
    return get_json_output(cmd)

def get_ims_images():
    """Generate a JSON dictionary of IMS images

    Returns:
        dict: JSON dictionary of IMS images found on a Cray CSM cluster
    """
    cmd = f"{cray_cmd} ims images list --format json"
    return get_json_output(cmd)

def save_product_catalog(target):
    """Save the cray-product-catalog data, and each of the sub-product data too.

    Args:
        target (String): Directory to save the product data to
    """

    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True) 

    catalog = get_product_catalog()
    output = os.path.join(target, "cray-product-catalog.json")
    with open(output, "w") as fp:
        fp.write(json.dumps(catalog, indent=2))

        logger.info(f"Exporting cray product catalog to {output}")

    if "data" in catalog:
        data = catalog["data"]
        for product in data:
            product_data = data[product]
            product_output = os.path.join(target, f"{product}.yaml")
            with open(product_output, "w") as fp:
                fp.write(product_data)
                logger.info(f"Exporting {product} data to {product_output}")

def save_ims_recipes(target):
    """Save IMS recipes found on the system

    Args:
        target (String): Directory to save the recipes to
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
    
    recipes = get_ims_recipes()

    for r in recipes:
        name = r["name"]
        id = r["id"]
        recipe_dir = os.path.join(target, name).replace(' ', '_')
        pathlib.Path(recipe_dir).mkdir(exist_ok=True)
        filename = os.path.join(target, f"{name}.json").replace(' ', '_')
        with open(filename, "w") as fp:
            fp.write(json.dumps(r, indent=2))    
            logger.info(f"Saved recipe {filename}")

        if "link" in r and r["link"] is not None and "path" in r["link"]:
            recipe_path = r["link"]["path"]
            recipe_basename = os.path.basename(recipe_path)
            recipe_bucket = recipe_path.split('/')[2]
            recipe_s3_path = "/".join(recipe_path.split('/')[3:])
            cmd = f"{cray_cmd} artifacts get {recipe_bucket} {recipe_s3_path} {recipe_dir}/{recipe_basename}"
            p = subprocess.Popen(cmd.split(), shell=False, stdout=PIPE, stderr=PIPE)
            rc = p.wait()
            if rc == 0:
                logger.info(f"Saved recipe artifact {recipe_dir}/{recipe_basename}")
            else:
                logger.debug(f"Warning: failed to save recipe artifact {recipe_dir}/{recipe_basename}. Is this a valid artifact path?")
        else:
            logger.warning(f"Warning: recipe {name} did not include valid link settings.")

def save_ims_images(target):
    """Save IMS images found on the system

    Args:
        target (String): Directory to save the images to
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
    
    images = get_ims_images()
    image_count = len(images)

    for i in images:
        name = i["name"]
        id = i["id"]

        image_detail = os.path.join(target, f"{id}.json")
        with open(image_detail, "w") as fp:
            fp.write(json.dumps(i, indent=2))
            logger.info(f"Saved image details {image_detail}")

        artifacts_dir = os.path.join(target, id)
        pathlib.Path(artifacts_dir).mkdir(exist_ok=True)

        if "link" in i and "path" in i["link"]:
            image_path = i["link"]["path"]
            image_bucket = image_path.split('/')[2]
            image_s3_path = os.path.dirname("/".join(image_path.split('/')[3:]))

            for image_component in ("manifest.json", "rootfs", "initrd"):
                component_s3_src = f"{image_s3_path}/{image_component}"
                component_target = f"{artifacts_dir}/{image_component}"

                cmd = f"{cray_cmd} artifacts get {image_bucket} {component_s3_src} {component_target}"
                p = subprocess.Popen(cmd.split(), shell=False, stdout=PIPE, stderr=PIPE)
                rc = p.wait()
                if rc == 0:
                    logger.info(f"Saved image artifact {component_target}")
                else:
                    logger.debug(f"Warning: failed to save image artifact {component_target}")
        else:
            logger.warning(f"Warning: no link or path data found in image detail for {name} ({id})")

def save_bss_configurations(target):
    """Save bss dumpstate and bootparams configured on the system

    Args:
        target (String): Directory to save the BSS configurations to
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)

    bss_cmds = [
            (f"{cray_cmd} bss bootparameters list --format json", "bootparameters.json"),
            (f"{cray_cmd} bss dumpstate list --format json", "dumpstate.json")
        ]
    for cmd, output in bss_cmds:
        data = get_json_output(cmd)
        output_fname = os.path.join(target, output)
        with open(output_fname, "w") as fp:
            logger.info(f"Exporting {output} to {output_fname}")
            json.dump(data, fp, indent=2)

def save_customizations_yaml(target):
    """Obtain and save the customizations.yaml file.  This command outputs YAML format, so do not
    use the get_json_output() method to cache it.  If this ends up getting used elsewhere, should
    consider making the cache function output format independant.

    Args:
        target (String): Directory to save the customizations.yaml file to
    """
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)

    # f-strings don't like back slashes, so append it separately.
    cmd = f"{kubectl_cmd} -n loftsman get secret site-init -o jsonpath" + "='{.data.customizations\\.yaml}'"
    p = subprocess.Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    rc = p.wait()
    if rc == 0:
        config = base64.decodestring(out).decode()
        output_fname = os.path.join(target, "customizations.yaml")
        with open(output_fname, "w") as fp:
            logger.info(f"Exporting Kubernetes site-init to {output_fname}")
            fp.write(config)

def save_certs(target):
    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)

    # f-strings don't like back slashes, so append it separately.
    out_cmds = [
        ("sealed_secrets.crt", f"{kubectl_cmd} -n kube-system get secret sealed-secrets-key -o jsonpath" + "='{.data.tls\\.crt}'"),
        ("sealed_secrets.key", f"{kubectl_cmd} -n kube-system get secret sealed-secrets-key -o jsonpath" + "='{.data.tls\\.key}'")
    ]
    for fname, cmd in out_cmds:
        p = subprocess.Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        rc = p.wait()
        if rc == 0:
            config = base64.decodestring(out).decode()
            output_fname = os.path.join(target, fname)
            with open(output_fname, "w") as fp:
                logger.info(f"Exporting {fname} to {output_fname}")
                fp.write(config)

def create_ncn_metadata(target):
    """Query SLS and HSM to build a representative ncn_metadata.csv file that could be used on a fresh 
    install of the same system to speed up deployment.

    Args:
        target (String): Path to directory to save the ncn_metadata.csv file to
    """

    if not os.path.exists(target):
        pathlib.Path(target).mkdir(parents=True, exist_ok=True)
    
    output_fname = os.path.join(target, "ncn_metadata.csv")
        
    ncn_masters = get_ncn_type("Master")
    ncn_workers = get_ncn_type("Worker")
    ncn_storage = get_ncn_type("Storage")
    
    with(open(output_fname, "w")) as fp:
        fp.write("Xname,Role,Subrole,BMC MAC,Bootstrap MAC, Bond0 MAC0, Bond0 MAC1\n")
        for ncns in ncn_storage, ncn_workers, ncn_masters:
            for xname, alias, role, subrole in ncns:
                bmc_xname = get_bmc_xname(xname)
                try:
                    bmc_mac = get_hsm_ethernet_macs(bmc_xname)[0]
                    bond0_mac = get_hsm_ethernet_macs(xname)[2]
                    bond1_mac = get_hsm_ethernet_macs(xname)[0]
                except IndexError:
                    # if no BMC MAC is identified, replace with placeholder.  ncn-m001 usually doesn't
                    # have a BMC MAC address recorded.
                    bmc_mac = bond0_mac = bond1_mac = "de:ad:be:ef:00:00"
                    logger.warning(f"Warning: No BMC MAC could be identified for {xname}.")
                logger.info(f"Created NCN metadata entry for {xname}")                
                fp.write(",".join((xname,role,subrole,bmc_mac,bond0_mac,bond1_mac)))
                fp.write("\n")
    logger.info(f"Wrote NCN data to {output_fname}")
                     
def get_ncn_list():
    """Query SLS to build a list of NCNs

    Returns:
        List: List of NCNs identified in SLS
    """
    ret = []
    cmd = "cray sls hardware list"
    data = get_json_output(cmd)

    for entry in data:
        try:
            if entry["TypeString"] == "Node":
                xname = entry["Xname"]
                role = entry["ExtraProperties"]["Role"]
                subrole = entry["ExtraProperties"]["SubRole"]
                alias = entry["ExtraProperties"]["Aliases"][0]
                ncn_data = (xname, alias, role, subrole)
                if role == "Management":
                    # Exclude Application node types
                    ret.append(ncn_data)
        except KeyError:
            continue
                
    return ret

def get_ncn_type(ncn_type=None):
    """Generate a list of NCNs of a specific type (Master, Worker, or Storage)

    Args:
        ncn_type (String, optional): Type to search SLS for matching entries. Defaults to None.

    Returns:
        List: List of NCNs which match the specified ncn_type
    """
    ret = []
    if ncn_type in ("Master", "Worker", "Storage"):
        ncns = get_ncn_list()
        for entry_tup in ncns:
            xname, hostnae, role, subrole = entry_tup
            if role == "Management" and subrole == ncn_type:
                ret.append(entry_tup)
    else:
        print(f"Warning: an invalid NCN type was specified: {ncn_type}")
    return ret

def get_bmc_xname(xname):
    """Derive a server's BMC xname from it's xname, by removing the last two characters

    Args:
        xname (String): Server xname to process into a BMC xname

    Returns:
        _type_: _description_
    """
    return xname[:-2]

def get_hsm_ethernet_macs(xname):
    """Query HSM for the associated ethernet MACs for a given xname

    Args:
        xname (String): Xname to search HSM for matching MAC addresses

    Returns:
        List: List of MAC addresses assocated to the specified xname
    """
    ret = []
    cmd = "cray hsm inventory ethernetInterfaces list"
    data = get_json_output(cmd)

    for e in data:
        if e["ComponentID"] == xname:
            ret.append(e["MACAddress"])
    return ret
        
def get_vcs_credentials():
    """Obtain the VCS credentials from k8s

    Returns:
        [String]: [Decoded password for VCS]
    """
    ret = None
    cmd = f"{kubectl_cmd} get secret -n services vcs-user-credentials --template={{{{.data.vcs_password}}}}"
    p = subprocess.Popen(cmd.split(), stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    rc = p.wait()
    if rc == 0:
        pw = base64.decodestring(out).decode()
        # print(pw)
        ret = pw
    return ret

def create_archive_file(fname, dirname, format="gz"):
    """Create a tar archive file of a directory.  Uses the Python tarfile
    module which supports GZ, BZ2 and XZ compression formats.

    Args:
        fname (string): Destination filename to create
        dirname (string): Directory path to create the archive from

    Exceptions:
        CollectArchiveException: Raised if an unsupported compression format is specified
    """
    pbanner(f"Creating archive file {fname}")
    support_compression_formats = ["gz", "bz2", "xz"]
    if format in support_compression_formats:
        logger.info(f'Creating archive file {fname} of {dirname}')
        curr_dir = os.getcwd()
        target_file = os.path.join(curr_dir, fname)
        os.chdir(dirname)
        try:
            with tarfile.open(target_file, f'w:{format}') as archive:
                for fname in os.listdir(dirname):
                    logger.debug(f"Adding {fname} to {target_file}")
                    archive.add(fname)
                # archive.list()
        except tarfile.CompressionError as e:
            msg = f'An error was occurred creating the archive file ({format})'
            logger.exception(msg)
            raise CollectArchiveException(msg)
        os.chdir(curr_dir)
    else:
        msg = f'The format specified ({format}) is not supported ({support_compression_formats})'
        logger.error(msg)
        raise CollectArchiveException(msg)

def pbanner(msg, length=78):
    """Print a formatted banner string padded to the specified length

    Args:
        msg (str): Message to embed in the banner string
        length (int, optional): Width to pad the string to. Defaults to 78.
    """
    if not quiet_flag:
        out = f"\n-- {msg} "
        out_len = length - len(out) - 4
        for x in range(out_len):
            out += "-"
        print(out)

def parse_args():
    """Encapsulate the processing of command line arguments to make them easier to manage.

    Returns:
        Object: Returns the resulting objects created by parse_args.
    """
    parser = argparse.ArgumentParser(description="Collect important details of a Shasta cluister")
    parser.add_argument("-b", "--branch", default="integration", help="Default branch to checkout in the git repo backup")
    parser.add_argument("-c", "--clean", action='store_true', help="Optionally clean up target_directory first")
    parser.add_argument("-e", "--certs", action='store_true', help="Save certificates used to generate sealsed-secrets in customizations.yaml")
    parser.add_argument("-g", "--golden", action='store_true', help="Create a 'golden configuration' state of a cluster")
    parser.add_argument("-i", "--images", action='store_true', help="Store IMS image artifacts")
    parser.add_argument("-o", "--output", help="Store the collected artifacts to a file instead of the target_dir")
    parser.add_argument("-s", "--skip", action='store_true', help="Skip long running collection steps")
    parser.add_argument("-f", "--switch_config", default="/etc/switch_admin.conf", help="Optional switch configuration file for GOSS tests")
    parser.add_argument("target_dir", help="Directory to store the important details of a cluster")
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    branch = args.branch
    clean_flag = args.clean
    golden_flag = args.golden
    skip_flag = args.skip
    image_flag = args.images
    target_dir = args.target_dir
    switch_config = args.switch_config
    cert_flag = args.certs

    if args.output:
        output_fname = args.output
    else:
        output_fname = None

    if golden_flag and output_fname is None:
        print("The golden configuration option requires the output option to be specified")
        exit(-1)
    else:
        logger.debug(f"Golden configuration option is enabled.  Snapshot will be saved to {output_fname}")

    # Make sure the CRAY_FORMAT environment variable is not set to prevent a problem
    # found when using the "--format json" option in various cray commands.
    try:
        del os.environ["CRAY_FORMAT"]
    except KeyError:
        pass

    # preparing for a future version which will need some additional support built-in
    csm_version = get_csm_version()
    if not csm_compatibility_check():
        logger.error(f"The version of CSM ({csm_version}) is not compatible with this version of the collect.py script")
        exit(-1)

    if not quiet_flag:
        print(f"Debugging: {debug_flag}")
        print (f"Storing Cluster Artifacts: {target_dir}")
        if skip_flag:
            print (f"Skipping time consuming collections due to skip_flag={skip_flag}")

    # command tuple: (function, parameter, description, skipable)
    commands = [
        (prep_target, target_dir, "Setup Collection Directory", False),
        (save_product_catalog, target_dir + "/inventory/catalog", "Collect Product Catalog", False),
        (gather_inventory, target_dir + "/inventory/systems", "Systems Inventory", False),
        (save_cfs_configurations, target_dir + "/configs/cfs", "CFS Configurations", False),
        (save_bos_sessiontemplates, target_dir + "/configs/bos", "Session Templates", False),
        (save_bootprep_files, target_dir + "/configs/bootprep", "SAT Bootprep Files", False),
        (save_bss_configurations, target_dir + "/configs/bss", "BSS Dumpstate and Bootparams", False),
        (save_customizations_yaml, target_dir + "/configs", "Kubernetes Site Init File", False),
        (create_ncn_metadata, target_dir + "/prep", "Generate NCN Metadata Seed File", False),
        (save_sls_dumpstate, target_dir + "/configs/sls", "SLS Dumpstate", False),
        (run_sshot_backup, target_dir + "/sshot", "Slingshot FMN Backups", True),
        (save_repos, target_dir + "/repos", "Configuration Repositories", False),
        (save_ims_recipes, target_dir + "/ims/recipes", "IMS Recipe Artifacts", False),
        (run_health_checks, target_dir + "/healthchecks", "Health Checks", True)
    ]

    if cert_flag:
        commands.append((save_certs, target_dir + "/configs/certs", "Sealed Secrets Certificates", False))
        logger.warning("The export of sensitive certificates has been enabled by the --certs flag.")

    if image_flag:
        commands.append((save_ims_images, target_dir + "/ims/images", "IMS Image Artifacts", False))            
        logger.warning("Warning: capturing IMS image artifacts is enabled by the --images flag.  This has")
        logger.warning("the potential to consume a LOT of disk space.  Be careful to not fill up a disk")
        logger.warning("on a NCN!")

    for command in commands:
        (fn, target, description, skipable) = command
        if skipable and skip_flag:
            continue
        else:
            pbanner(description)
            fn(target)

    if output_fname is not None:
        if output_fname.endswith("bz2"):
            archive_format = "bz2"
        elif output_fname.endswith("xz"):
            archive_format = "xz"
        else:
            # default
            archive_format = "gz"
        try:
            create_archive_file(output_fname, target_dir, archive_format)
            if golden_flag:
                logger.info(f"Golden configuration option selected.  Successfully created {output_fname}, removing {target_dir}")
                shutil.rmtree(target_dir)
        except (CollectArchiveException, FileNotFoundError) as e:
            logger.error(e)
        
    else:
        logger.debug("No output filename specified, skipping archive")

