#!/usr/bin/env python2
"""Build Leonardo and deploy it to Google Compute Engine.

See automation/gce-deploy/README.md for instructions.
"""

from __future__ import print_function

import argparse
import contextlib
import datetime
import getpass
import json
import os
import random
import re
import string
import subprocess
import sys
import tempfile
import time


LOCAL_USER = getpass.getuser()
RANDSUFFIX = ''.join(random.choice(string.ascii_uppercase) for _ in range(4))
RANDPASSWORD = ''.join(random.choice(string.ascii_uppercase) for _ in range(14))
TODAY_SUFFIX = datetime.datetime.now().strftime('%b%d')
SQL_INSTANCE_NAME = ("leo-sql-%s-%s" % (TODAY_SUFFIX, RANDSUFFIX)).lower()
GCE_INSTANCE_NAME = ("leo-server-%s-%s" % (TODAY_SUFFIX, RANDSUFFIX)).lower()
SCRIPT_DIR = os.path.dirname(__file__)
LEO_FIREWALL_RULE = 'leonardo-server'

with open(os.path.join(SCRIPT_DIR, 'instance_init.sh'), 'r') as init_file:
    GCE_INIT_SCRIPT_LOGIC = init_file.read()

REQUIRED_APIS = [
    'cloudkms.googleapis.com',
    'containerregistry.googleapis.com',
    'sqladmin.googleapis.com',  # Required for SQL proxy.
    'compute.googleapis.com',
]


SSL_FILE_FLAGS = ['rootca_key', 'ssl_cert', 'ssl_key', 'ssl_ca_bundle', 'ssl_test_file']

# Regex for validating GCS paths in the form of 'gs://bucket/path'.
GS_REGEX = r'^gs://[a-zA-Z0-9_\.-]{1,100}/.{1,200}$'

GCE_INIT_SCRIPT_VARS = """
#!/bin/bash

set -e

# Set variables used by the invariant part of the init script
# Required args.
SQL_PROXY_PATH=/usr/bin/cloud_sql_proxy
SERVER_HOST={server_host}
REMOTE_USER={user}
LEONARDO_SERVER_IMAGE={docker_image}

# Optional args used for secure remote resources.
SERVER_SSL_KEY="{server_ssl_key}"
SERVER_SSL_CERT="{server_ssl_cert}"
SERVER_CA_BUNDLE="{server_ca_bundle}"
INTERNAL_ROOT_CA="{rootca_key}"
KMS_KEY="{kms_key}"
KMS_KEYRING="{kms_keyring}"
KMS_PROJECT="{kms_project}"
KMS_LOCATION="{kms_location}"
SSL_TEST_FILE="{ssl_test_file}"
"""


class Print(object):
    """Print to the console in color!"""

    @staticmethod
    def GN(text):  # pylint: disable=invalid-name,missing-docstring
        print('\033[92m' + text + '\033[0m')

    @staticmethod
    def YL(text):  # pylint: disable=invalid-name,missing-docstring
        print('\033[93m' + text + '\033[0m')


def parse_arguments():  # pylint: disable=too-many-statements
    """Create and parse argument namespace.

    The only required arguments are project and zone. Other arguments
    change auto-deploy behaviors but sane defaults are set.
    """
    parser = argparse.ArgumentParser(
        description=('Deploy Leonardo to Google Compute Engine\nSee automation'
                     '/gce-deploy/README.md for detailed instructions'),
        formatter_class=argparse.RawTextHelpFormatter)

    # General purpose arguments.
    parser.add_argument(
        '--project',
        metavar='PROJECT',
        required=True,
        help='(required) GCP project for deploying leonardo.')
    parser.add_argument(
        '--region',
        metavar='REGION',
        required=True,
        help='(required) GCP compute region for deployment.')
    parser.add_argument(
        '--host',
        metavar='HOST',
        required=True,
        help='(required) The dns name / host name for Leonardo.')
    parser.add_argument(
        '--oauth2-client-id',
        metavar='CLIENT-ID',
        required=True,
        help='(required) OAuth 2.0 client ID, see gce-deploy/README.md.')
    buildgroup = parser.add_mutually_exclusive_group()
    buildgroup.add_argument(
        '--nobuild',
        dest='build',
        default=True,
        action='store_false',
        help='Build and push the docker images.')
    buildgroup.add_argument(
        '--build-and-exit',
        dest='build_and_exit',
        default=False,
        action='store_true',
        help='Build and push the docker images, then exit deploy.py without deploy VMs.')
    buildgroup.add_argument(
        '--build',
        dest='build',
        action='store_true')

    # Arguments that are specific to Compute Engine.
    gce = parser.add_argument_group(
        title='Compute Engine Config',
        description='Options to control GCE instance creation.')
    gce.add_argument(
        '--https-only', dest='https_only', default=False, action='store_true',
        help='Do not make a custom firewall rule, use the canned https tag.')
    gce.add_argument(
        '--zone',
        metavar='ZONE',
        required=False,
        default='',
        help='Compute zone (default: us a random zone in the deploy region).')
    gce.add_argument(
        '--gce-disk-size',
        metavar='ZONE',
        required=False,
        default=200,
        type=int,
        help='Compute disk size (default: 200GB).')
    gce.add_argument(
        '--gce-instance-type',
        metavar='TYPE',
        required=False,
        default='n1-standard-2',
        help='Compute instance type (default: n1-standard-2).')
    gce.add_argument(
        '--service-account',
        metavar='SERVICE-ACCOUNT',
        required=False,
        default='default',
        help='Compute service account (default: default service account).')

    ssl = parser.add_argument_group(
        title='SSL Certificate Config',
        description='Options to control SSL certs.')
    ssl.add_argument(
        '--kms-project',
        metavar='PROJECT',
        required=False,
        default='',
        help='project of kms keyring (defaults to --project)')
    ssl.add_argument(
        '--kms-location',
        metavar='LOCATION',
        required=False,
        default='global',
        help='Location of kms keyring (defaults to global)')
    ssl.add_argument(
        '--kms-keyring',
        metavar='KEYRING',
        required=False,
        default='',
        help='Name of kms keyring')
    ssl.add_argument(
        '--kms-key',
        metavar='KEY',
        required=False,
        default='',
        help='Name of kms key')
    ssl.add_argument(
        '--rootca-key',
        metavar='GCS-PATH',
        required=False,
        default='',
        help='GCS path of the encrypted root CA key file.')
    ssl.add_argument(
        '--ssl-cert',
        metavar='GCS-PATH',
        required=False,
        default='',
        help='GCS path of the encrypted front-end certificate.')
    ssl.add_argument(
        '--ssl-key',
        metavar='GCS-PATH',
        required=False,
        default='',
        help='GCS path of the encrypted front-end key.')
    ssl.add_argument(
        '--ssl-ca-bundle',
        metavar='GCS-PATH',
        required=False,
        default='',
        help='GCS path of the encrypted front-end CA bundle.')
    ssl.add_argument(
        '--ssl-test-file',
        metavar='GCS-PATH',
        required=False,
        default='',
        help='Test file.')

    sql = parser.add_argument_group(
        title='Cloud SQL Config',
        description='Options to control Cloud SQL instance assignment or creation.')
    sql.add_argument(
        '--use-existing-sql',
        default=False,
        action='store_true',
        help='Use a Cloud SQL instance that is already provisioned.')
    sql.add_argument(
        '--cloud-sql-name',
        metavar='SQL-NAME',
        default='',
        help='Cloud SQL instance name (default generates name based on date).')
    sql.add_argument(
        '--cloud-sql-password',
        metavar='PASSWORD',
        default='',
        help='Cloud SQL password (default generates random password).')
    sql.add_argument(
        '--cloud-sql-region',
        metavar='REGION',
        default='',
        help='Cloud SQL region. Defaults to value of "--region".')
    sql.add_argument(
        '--cloud-sql-project',
        metavar='PROJECT',
        default='',
        help='Cloud SQL region. Defaults to value of "--project".')
    sql.add_argument(
        '--sql-port',
        metavar='PORT',
        default=3306,
        type=int,
        help='Port used by sql proxy (default: 3306).')

    parser.set_defaults(includes_ssl_files=False)
    args = parser.parse_args()

    # Set branch, from git if git repo is active. Otherwise use 'dev'.
    try:
        args.branch = subprocess.check_output(
            ['git', 'rev-parse', '--abbrev-ref', 'HEAD']
        ).strip()
    except subprocess.CalledProcessError:
        args.branch = 'dev'
    if args.build_and_exit:
        return args

    args = ssl_args_rewrite_validate(args)

    # Determine zone.
    args.zone = get_zone_from_flag_values(args.region, args.zone)

    # Preprocess sql flags - default values come from required flags.
    if not args.cloud_sql_name:
        args.cloud_sql_name = SQL_INSTANCE_NAME
    if not args.cloud_sql_region:
        args.cloud_sql_region = args.region
    if not args.cloud_sql_project:
        args.cloud_sql_project = args.project
    if not args.cloud_sql_password:
        args.cloud_sql_password = RANDPASSWORD
    return args


def ssl_args_rewrite_validate(args):
    """Validate SSL args, then rewrite any cross-flag defaults."""
    # Check that file paths are present and well-formed values. In that case,
    # set the user_hidden 'includes_ssl_files' flag to True.
    valid_kms_args = True
    for sslflag in SSL_FILE_FLAGS:
        argvalue = getattr(args, sslflag)
        if argvalue:
            args.includes_ssl_files = True
            if not re.match(GS_REGEX, argvalue):
                warn = '%s must be a GCS path (ex: gs://BUCKET/OBJECT)'
                warn += '\nfound %s instead.'
                warn %= (sslflag, argvalue)
                Print.YL(warn)
                valid_kms_args = False
    # When passing SSL files, some default values may come from
    # global flags. Reset those defaults and raise errors if required
    # flags are missing.
    if args.includes_ssl_files:
        args.kms_project = args.kms_project or args.project
        if not args.kms_keyring:
            Print.YL('If including SSL files, you must set --kms-keyring.')
            valid_kms_args = False
        if not args.kms_key:
            Print.YL('If including SSL files, you must set --kms-key.')
            valid_kms_args = False
    if not valid_kms_args:
        Print.YL('SSL flags were missing or incorrect, see gce-deploy/README.md')
        sys.exit(1)
    return args


def validate_keystore_key(args):
    """Use gcloud to validate that specified KMS key exists."""
    expected_name = 'keyRings/%s/cryptoKeys/%s'
    expected_name %= (args.kms_keyring, args.kms_key)
    describe_output = ''
    try:
        describe_output = subprocess.check_output(
            ['gcloud', 'kms', 'keys', 'describe', args.kms_key,
             '--project', args.kms_project,
             '--location', args.kms_location,
             '--keyring', args.kms_keyring,
             '--format', 'value(name)'])
    except subprocess.CalledProcessError:
        pass
    if expected_name in describe_output:
        return
    # Print warning and exit if output did not include the key.
    warning = 'KMS key "%s" not found in keyring=%s project=%s location=%s'
    warning %= (args.kms_key,
                args.kms_keyring,
                args.kms_project,
                args.kms_location)
    Print.YL(warning)
    sys.exit(1)


def ssh_key_propagation_call_retry(cmd):
    """Execute gcloud and retry until timeout."""
    time_waited = 0
    max_ssh_wait = 180
    print('Waiting up to %d seconds for SSH key propagation' % max_ssh_wait)
    while time_waited < max_ssh_wait:
        time.sleep(5)
        time_waited += 6  # Each try takes about a second.
        # Each retry creates noisy stderr messages that should not be shown.
        with open(os.devnull, 'w') as fnull:
            try:
                return subprocess.check_call(cmd, stderr=fnull)
            except subprocess.CalledProcessError as err:
                pass
        sys.stdout.write('.')
        sys.stdout.flush()
    # Raise this error if we exit the retry loop without returning.
    print('Failed to connect to instance in %d seconds.' % max_ssh_wait)
    raise err


def _to_gcr_path(project, image_name, tag):
    """Convert project, image name and tag to a valid GCR name."""
    image_project = project.replace(':', '/')
    return 'gcr.io/' + image_project + '/' + image_name + ':' + tag


def gcloud_json(cmd_list):
    """Execute gcloud with json format, returning the parsed results."""
    if any(['--format' in field for field in cmd_list]):
        raise ValueError('Format must be controlled by this function')
    cmd_list = [c for c in cmd_list]  # Copy list to prevent mutation.
    cmd_list.append('--format=json')
    raw_data = subprocess.check_output(cmd_list)
    return json.loads(raw_data)


def enable_gcp_apis(project):
    """Check and, with consent, enable required GCP APIs."""
    active_services = subprocess.check_output(
        ['gcloud', 'services', 'list',
         '--project', project,
         '--format', 'value(serviceName)'])
    active_services = [s.strip() for s in active_services.splitlines()]
    needed_services = []
    for service in REQUIRED_APIS:
        if service not in active_services:
            needed_services.append(service)
    if not needed_services:
        return
    Print.YL('Several required GCP services are disabled, enable them now? [y/N]')
    for service in needed_services:
        print('  - %s' % service)

    response = raw_input(': ')
    if not re.match('^y(es)?$', response.lower().strip()):
        Print.YL('WARNING: this may result in unexpected behaviors or errors.')
        return
    for service in needed_services:
        subprocess.check_call(
            ['gcloud', 'services', 'enable',
             service,
             '--project', project])


def select_eligible_ip(project, region):
    """Interact with a shell user to select or create an IP.

    This function implements a user interaction where eligible
    reserved IP addresses may be listed and offered as the GCE IP.
    If no IPs are available or the user elects not to use an
    existing IP, the user can create a new IP.

    While this script's interaction can be overridden by flags,
    it's default behavior is to prompt the user for a decision.
    """
    Print.GN('Starting IP address selection')
    reuse_reserved = False
    # Check if reserved IPs exist
    list_command = ['gcloud', 'compute', 'addresses', 'list', '--project', project]
    ip_data = gcloud_json(list_command)
    ip_data_filtered = [ip for ip in ip_data
                        if ip['status'] == 'RESERVED'
                        and ip['region'].endswith(region)]
    # Check if the user wants to reuse an IP.
    if ip_data_filtered:
        print('Several existing reserved IPs were found.')
        response = raw_input('Would you like to use one of them? [y/N] ')
        if re.match('^y(es)?$', response.lower().strip()):
            reuse_reserved = True
    # List IPs to reuse and get selection.
    while reuse_reserved:
        print('\n  Unused static IPs in %s:' % region)
        for i, ip_data in enumerate(ip_data_filtered):
            print('  %d. %s - %s' % (i+1, ip_data['name'], ip_data['address']))
        print('\nEnter the number of the IP (or "-1" to cancel selection)')
        response = raw_input('')
        if not re.match(r'^-?\d+$', response):
            print('Entry must be a number. Restarting IP selection.')
            continue
        response = int(response)
        if response == -1:
            break
        if response <= 0 or len(ip_data_filtered) < response:
            print('Select a number between 1 and %d' % len(ip_data_filtered))
            continue
        return ip_data_filtered[response - 1]['address']
    # To get here, the user must have not selected an IP.
    response = raw_input('Would you like to create a reserved IP? [y/N] ')
    if not re.match('^y(es)?$', response.lower().strip()):
        raise ValueError('Leonardo server requires a reserved IP.')
    addr_name = 'leonardo-%s' % RANDSUFFIX.lower()
    subprocess.check_call(
        ['gcloud', 'compute', 'addresses', 'create', addr_name,
         '--description', 'Address used for leonardo deployment on %s' % TODAY_SUFFIX,
         '--region', region,
         '--project', project])
    for ip_info in gcloud_json(list_command):
        if ip_info['name'] == addr_name:
            return ip_info['address']
    raise ValueError('Could not find or create a reserved IP address.')


def get_zone_from_flag_values(region, zone):
    """Get zones from flag values.

    If the zone was provided, validate that it matches the region. Otherwise
    select a zone randomly in the specified region.
    """
    zones = subprocess.check_output(
        ['gcloud', 'compute', 'zones', 'list', '--format', 'value(name)'])
    filtered_zones = [z.strip() for z in zones.splitlines()
                      if z.strip().startswith(region)]
    if zone and zone not in filtered_zones:
        raise ValueError('zone: "%s" not in region %s' % (zone, region))
    if zone:
        return zone
    return random.choice(filtered_zones)


def create_database(args):
    """Create the Cloud SQL database instance."""
    Print.GN('Creating and configuring Cloud SQL.')
    connection_name = '%s:%s:%s' % (
        args.cloud_sql_project, args.cloud_sql_region, args.cloud_sql_name)
    subprocess.check_output(
        ['gcloud', 'sql', 'instances', 'create',
         args.cloud_sql_name,
         '--project', args.cloud_sql_project,
         '--activation-policy', 'ALWAYS',
         '--tier', 'db-n1-standard-1',
         '--assign-ip',
         '--no-backup',
         '--database-version', 'MYSQL_5_6',
         '--region', 'us-west1',
         '--storage-auto-increase',
         '--storage-size', '10',
         '--storage-type', 'HDD'])
    subprocess.check_output(['gcloud', 'sql', 'users', 'set-password',
                             'root', '%',
                             '--password', args.cloud_sql_password,
                             '--instance', args.cloud_sql_name,
                             '--project', args.cloud_sql_project])
    subprocess.check_output(['gcloud', 'sql', 'databases', 'create',
                             'leonardo',
                             '--instance', args.cloud_sql_name,
                             '--project', args.cloud_sql_project])
    print('Successfully created instance: %s' % connection_name)
    return connection_name


def create_firewall_rule(project):
    """Create a firewall rule for Leonardo."""
    listed_rules = subprocess.check_output(
        ['gcloud', 'compute', 'firewall-rules', 'list',
         '--format', 'value(name)',
         '--filter', 'name=%s' % LEO_FIREWALL_RULE,
         '--project', project])
    if LEO_FIREWALL_RULE in listed_rules:
        return
    Print.GN('Creating firewall rule for Leonardo VM.')
    subprocess.check_call(
        ['gcloud', 'compute', 'firewall-rules', 'create',
         LEO_FIREWALL_RULE,
         '--allow', 'tcp:80,tcp:443',
         '--priority', '900',
         '--target-tags', LEO_FIREWALL_RULE,
         '--project', project])


def create_gce_instance(args, ip_address):
    """Create Leonardo GCE instance."""
    Print.GN('Creating GCE VM.')
    instance_name = GCE_INSTANCE_NAME.lower()
    firewall_tag = 'https-server' if args.https_only else LEO_FIREWALL_RULE
    cmd = ['gcloud', 'compute', 'instances', 'create',
           instance_name,
           '--image-family', 'ubuntu-1604-lts',
           '--image-project', 'ubuntu-os-cloud',
           '--project', args.project,
           '--scopes', 'cloud-platform',
           '--zone', args.zone,
           '--address', ip_address,
           '--machine-type', args.gce_instance_type,
           '--service-account', args.service_account,
           '--boot-disk-size', str(args.gce_disk_size),
           '--labels', 'instance-creator=leonardo-easy-deploy',
           '--tags', firewall_tag,
           '--boot-disk-auto-delete',
           # 'metadata-from-file' must be the last argument.
           '--metadata-from-file']
    with tempfile.NamedTemporaryFile(mode='w') as startup_file:
        gce_vars = GCE_INIT_SCRIPT_VARS.format(
            server_host=args.host,
            user=LOCAL_USER,
            docker_image=_to_gcr_path(args.project, 'leonardo', args.branch),
            server_ssl_key=args.ssl_key,
            server_ssl_cert=args.ssl_cert,
            server_ca_bundle=args.ssl_ca_bundle,
            rootca_key=args.rootca_key,
            kms_key=args.kms_key,
            kms_keyring=args.kms_keyring,
            kms_project=args.kms_project,
            kms_location=args.kms_location,
            ssl_test_file=args.ssl_test_file,
        )
        startup_file.write(gce_vars + '\n' + GCE_INIT_SCRIPT_LOGIC)
        startup_file.flush()
        cmd.append('startup-script=%s' % startup_file.name)
        subprocess.check_call(cmd)
    # Startup script always takes time during which the instance
    # is unavailable.
    time.sleep(15)
    print('Successfully created instance: %s' % instance_name)
    return instance_name


def build_jar_and_push(project, branch):
    """Build the leo JAR and docker image, pushing to GCR."""
    Print.GN('Building JAR and pushing image.')
    current_env = os.environ.copy()
    current_env['BRANCH'] = branch
    subprocess.check_call([
        os.path.join(SCRIPT_DIR, '../../docker/build.sh'),
        'jar',
        '-p', project,
        '-d', 'push',
        '-r', 'gcr',
    ], env=current_env)


def email_for_default_sa(project):
    """Convert default service account to account email."""
    sa_structs = gcloud_json(['gcloud', 'iam',
                              'service-accounts', 'list',
                              '--project', project])
    for sa_struct in sa_structs:
        if 'compute@developer' in sa_struct['email']:
            return sa_struct['email']
    Print.YL('Could not find compute default service account!')
    Print.YL('See `gcloud iam service-accounts list`.')
    sys.exit(1)


def validate_service_account_keys(service_account, project):
    """Validate service account, and SA keys, rewriting email if needed."""
    Print.GN('Validating service account and keys.')
    if service_account == 'default':
        service_account = email_for_default_sa(project)
    try:
        user_keys = gcloud_json([
            'gcloud', 'iam', 'service-accounts', 'keys', 'list',
            '--iam-account', service_account, '--managed-by=user'])
    except subprocess.CalledProcessError:
        Print.YL('Could not list keys for: %s' % service_account)
        Print.YL('Either it does not exist or you lack permissions to access keys.')
        sys.exit(1)
    if not user_keys:
        return service_account
    Print.YL('Found %d keys for this service account. Service accounts' % len(user_keys))
    Print.YL('may only have a limited number of user-managed keys.\n\n'
             'Would you like to revoke existing keys (this may break\napplications'
             ' that rely on a key file)\n[y/N]')
    response = raw_input('')
    if not re.match('^y(es)?$', response.lower().strip()):
        return service_account
    print('Purging keys.')
    for key_strict in user_keys:
        key_id = key_strict['name'].rpartition('/')[2]
        subprocess.check_call(['gcloud', 'iam', 'service-accounts', 'keys', 'delete',
                               key_id,
                               '--iam-account', service_account,
                               '--quiet'])
    return service_account


@contextlib.contextmanager
def get_service_acct_pem_file(args):
    """Context manager to access an unencrypted PEM file."""
    # Now that we have the email
    with tempfile.NamedTemporaryFile() as ptwelve:
        with tempfile.NamedTemporaryFile() as pem:
            subprocess.check_call([
                'gcloud', 'iam', 'service-accounts', 'keys', 'create',
                ptwelve.name,
                '--key-file-type=p12',
                '--project', args.project,
                '--iam-account', args.service_account,
            ])
            subprocess.check_call([
                'openssl', 'pkcs12',
                '-in', ptwelve.name,
                '-out', pem.name,
                '-nodes',
                '-passin', 'pass:notasecret',
            ])
            yield pem.name


@contextlib.contextmanager
def generate_config_from_tmpl(compose_path, args, sql_conn=''):
    """Context manager to make configs for upload.

    A number of config template variables are shared between config files.
    This context manager does the replacement of each template config
    and writes to a temp file. The name of that temp file is returned
    while within the context, then the tempfile is cleaned up.

    Args:
        config_path: (str) path to config with template variables.
        args: (argparse.Namespace) parsed and validated arguments.
        sql_conn: (str) full Cloud SQL connection string, only needed
            for some config files.

    Yields:
        name of temporary config file.
    """
    raw_conf = open(compose_path, 'r').read()
    replace_map = {
        'TEMPLATE_VAR_PROJECT': args.project,
        'TEMPLATE_VAR_DOMAIN': args.host,
        'TEMPLATE_VAR_SERVICE_ACCOUNT': args.service_account,
        'TEMPLATE_VAR_OAUTH2_CLIENT_ID': args.oauth2_client_id,
        'TEMPLATE_VAR_SQL_INSTANCE_CONN': sql_conn,
        'TEMPLATE_VAR_DBPASS': args.cloud_sql_password,
        'TEMPLATE_VAR_DOCKER_PROJECT': args.project.replace(':', '/'),
        'TEMPLATE_VAR_DOCKER_TAG': args.branch,
    }
    for template_var, value in replace_map.items():
        raw_conf = raw_conf.replace(template_var, value)
    with tempfile.NamedTemporaryFile() as real_conf:
        real_conf.write(raw_conf)
        real_conf.flush()
        yield real_conf.name


def configure_gce_instance(instance_name, db_conn_name, args):
    """Send config files to the launched GCE instance."""
    Print.GN('Configuring GCE VM.')
    # Config tuples: (destination user, destination path, source path).
    configs = [
        ('~/docker-compose.yml', os.path.join(SCRIPT_DIR, 'docker-compose.yml')),
        ('~/app/site.conf', os.path.join(SCRIPT_DIR, 'site.conf')),
        ('~/app/leonardo.conf', os.path.join(SCRIPT_DIR, 'leonardo.conf')),
    ]
    for dest, source in configs:
        with generate_config_from_tmpl(source, args, sql_conn=db_conn_name) as site_conf:
            cmd = ('gcloud', 'compute', 'scp',
                   site_conf,
                   'ubuntu@%s:%s' % (instance_name, dest),
                   '--zone', args.zone,
                   '--project', args.project)
            # Run call with retry wrapper - ssh keys may take time to propagate.
            ssh_key_propagation_call_retry(cmd)
    # leonardo-account.pem
    with get_service_acct_pem_file(args) as pem_file_name:
        cmd = ('gcloud', 'compute', 'scp', pem_file_name,
               'ubuntu@%s:~/app/leonardo-account.pem' % instance_name,
               '--zone', args.zone,
               '--project', args.project)
        subprocess.check_call(cmd)


def main():
    """Main function."""
    args = parse_arguments()
    if args.includes_ssl_files:
        validate_keystore_key(args)
    if args.build or args.build_and_exit:
        build_jar_and_push(args.project, args.branch)
        if args.build_and_exit:
            print('Building complete, skipping cloud deploy.')
            exit(0)
    enable_gcp_apis(args.project)

    args.service_account = validate_service_account_keys(
        args.service_account, args.project)

    ip_address = select_eligible_ip(args.project, args.region)
    if args.host == 'IP':
        args.host = ip_address
    if args.use_existing_sql:
        db_conn_name = '{project}:{region}:{name}'.format(
            project=args.project,
            region=args.cloud_sql_region,
            name=args.cloud_sql_name)
    else:
        db_conn_name = create_database(args)
    if not args.https_only:
        create_firewall_rule(args.project)
    gce_instance_name = create_gce_instance(args, ip_address)
    configure_gce_instance(gce_instance_name, db_conn_name, args)
    Print.GN('You have successfully deployed Leonardo. You can run Leonardo')
    Print.GN('and view logs using the following gcloud command:\n')
    run_cmd = ('  gcloud compute ssh ubuntu@{name} \\\n'
               '      --zone="{zone}" \\\n'
               '      --project="{project}" \\\n      -- \\\n'
               '      "docker-compose up"\n')
    run_cmd = run_cmd.format(name=gce_instance_name, zone=args.zone, project=args.project)
    Print.GN(run_cmd)
    Print.GN('Once run with the command above, you can visit https://%s/ \n' % args.host)
    Print.GN('Be sure to add "https://%s/o2c.html" to the list of' % args.host)
    Print.GN('authorized redirect URIs of your OAuth2 Client.')



if __name__ == '__main__':
    main()
