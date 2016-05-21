#!/usr/bin/python

import argparse
import logging
import requests
import json
import subprocess
import sys
import tempfile

URL = 'https://api.github.com/repos/rqlite/rqlite/releases'

def run(command, allow_failure=False, shell=False):
    """
    Run shell command (convenience wrapper around subprocess).
    """
    out = None
    logging.debug("{}".format(command))
    try:
        if shell:
            out = subprocess.check_output(command, stderr=subprocess.STDOUT, shell=shell)
        else:
            out = subprocess.check_output(command.split(), stderr=subprocess.STDOUT)
        out = out.decode('utf-8').strip()
        # logging.debug("Command output: {}".format(out))
    except subprocess.CalledProcessError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e.output))
            return None
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e.output))
            sys.exit(1)
    except OSError as e:
        if allow_failure:
            logging.warn("Command '{}' failed with error: {}".format(command, e))
            return out
        else:
            logging.error("Command '{}' failed with error: {}".format(command, e))
            sys.exit(1)
    else:
        return out

def get_system_arch():
    """Retrieve current system architecture.
    """
    arch = os.uname()[4]
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "386":
        arch = "i386"
    elif 'arm' in arch:
        # Prevent uname from reporting full ARM arch (eg 'armv7l')
        arch = "arm"
    return arch

def get_system_platform():
    """Retrieve current system platform.
    """
    if sys.platform.startswith("linux"):
        return "linux"
    else:
        return sys.platform

def create_temp_dir(prefix=None):
    """
    Create temporary directory with optional prefix.
    """
    if prefix is None:
        return tempfile.mkdtemp(prefix="{}-build.".format(PACKAGE_NAME))
    else:
        return tempfile.mkdtemp(prefix=prefix)

def get_current_commit(short=False):
    """Retrieve the current git commit.
    """
    command = None
    if short:
        command = "git log --pretty=format:'%h' -n 1"
    else:
        command = "git rev-parse HEAD"
    out = run(command)
    return out.strip('\'\n\r ')

def get_current_branch():
    """Retrieve the current git branch.
    """
    command = "git rev-parse --abbrev-ref HEAD"
    out = run(command)
    return out.strip()

def get_go_version():
    """Retrieve version information for Go.
    """
    out = run("go version")
    matches = re.search('go version go(\S+)', out)
    if matches is not None:
        return matches.groups()[0].strip()
    return None

def build(version=None,
          platform=None,
          arch=None,
          nightly=False,
          clean=False,
          outdir=".",
          tags=[]):
    """Build each target for the specified architecture and platform.
    """
    logging.info("Starting build for {}/{}...".format(platform, arch))
    logging.info("Using Go version: {}".format(get_go_version()))
    logging.info("Using git branch: {}".format(get_current_branch()))
    logging.info("Using git commit: {}".format(get_current_commit()))
    if len(tags) > 0:
        logging.info("Using build tags: {}".format(','.join(tags)))

    logging.info("Sending build output to: {}".format(outdir))
    if not os.path.exists(outdir):
        os.makedirs(outdir)
    elif clean and outdir != '/' and outdir != ".":
        logging.info("Cleaning build directory '{}' before building.".format(outdir))
        shutil.rmtree(outdir)
        os.makedirs(outdir)

    logging.info("Using version '{}' for build.".format(version))

    tmp_build_dir = create_temp_dir()
    for target, path in targets.items():
        logging.info("Building target: {}".format(target))
        build_command = ""

        # Handle variations in architecture output
        if arch == "i386" or arch == "i686":
            arch = "386"
        elif "arm" in arch:
            arch = "arm"
        build_command += "GOOS={} GOARCH={} ".format(platform, arch)

        if "arm" in arch:
            if arch == "armel":
                build_command += "GOARM=5 "
            elif arch == "armhf" or arch == "arm":
                build_command += "GOARM=6 "
            elif arch == "arm64":
                # TODO(rossmcdonald) - Verify this is the correct setting for arm64
                build_command += "GOARM=7 "
            else:
                logging.error("Invalid ARM architecture specified: {}".format(arch))
                logging.error("Please specify either 'armel', 'armhf', or 'arm64'.")
                return False
        build_command += "go build -o {} ".format(os.path.join(outdir, target))
        if race:
            build_command += "-race "
        if len(tags) > 0:
            build_command += "-tags {} ".format(','.join(tags))
        if "1.4" in get_go_version():
            if static:
                build_command += "-ldflags=\"-s -X main.version {} -X main.branch {} -X main.commit {}\" ".format(version,
                                                                                                                  get_current_branch(),
                                                                                                                  get_current_commit())
            else:
                build_command += "-ldflags=\"-X main.version {} -X main.branch {} -X main.commit {}\" ".format(version,
                                                                                                               get_current_branch(),
                                                                                                               get_current_commit())

        else:
            # Starting with Go 1.5, the linker flag arguments changed to 'name=value' from 'name value'
            if static:
                build_command += "-ldflags=\"-s -X main.version={} -X main.branch={} -X main.commit={}\" ".format(version,
                                                                                                                  get_current_branch(),
                                                                                                                  get_current_commit())
            else:
                build_command += "-ldflags=\"-X main.version={} -X main.branch={} -X main.commit={}\" ".format(version,
                                                                                                               get_current_branch(),
                                                                                                               get_current_commit())
        build_command += path
        start_time = datetime.utcnow()
        run(build_command, shell=True)
        end_time = datetime.utcnow()
        logging.info("Time taken: {}s".format((end_time - start_time).total_seconds()))
    return True

class Parser(object):
    def __init__(self, text):
        self.text = text

    def parse(self):
        self.json = json.loads(self.text)

    def release_id(self, tag):
        for release in self.json:
            if release['tag_name'] == tag:
                return release['id']
        return None

    def upload_url(self, tag):
        for release in self.json:
            if release['tag_name'] == tag:
                return release['upload_url']
        return None

def parse_args():
    parser = argparse.ArgumentParser(description='Publish a rqlite release to GitHub.')
    parser.add_argument('tag', metavar='TAG', type=str,
                       help='tag in the form "vX.Y.Z"')
    parser.add_argument('token', metavar='TOKEN', type=str,
                       help='GitHub API token')

    return parser.parse_args()

def main():
    args = parse_args()

    r = requests.get(URL)
    if r.status_code != 200:
        print 'failed to download release information'
        sys.exit(1)

    p = Parser(r.text)
    p.parse()
    release_id = p.release_id(args.tag)
    if release_id == None:
        print 'unable to determine release ID for tag %s' % args.tag
        sys.exit(1)

if __name__ == '__main__':
    main()
