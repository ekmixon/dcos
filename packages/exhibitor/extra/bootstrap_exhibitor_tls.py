#!/opt/mesosphere/bin/python
import os
import socket
import subprocess
import sys

from pathlib import Path
from urllib.parse import urlparse


TLS_ARTIFACT_LOCATION = '/var/lib/dcos/exhibitor-tls-artifacts'
CSR_SERVICE_CERT_PATH = '/tmp/root-cert.pem'
PRESHAREDKEY_LOCATION = '/var/lib/dcos/.dcos-bootstrap-ca-psk'
EXHIBITOR_TLS_TMP_DIR = '/var/lib/dcos/exhibitor/.pki'
BOOTSTRAP_CA_BINARY = '/opt/mesosphere/bin/dcos-bootstrap-ca'


def invoke_detect_ip():
    try:
        ip = subprocess.check_output(
            ['/opt/mesosphere/bin/detect_ip']).strip().decode('utf-8')
    except subprocess.CalledProcessError as e:
        print(f"check_output exited with {e}")
        sys.exit(1)
    try:
        socket.inet_aton(ip)
        return ip
    except socket.error as e:
        print(f"inet_aton exited with {e}. {ip} is not a valid IPv4 address")
        sys.exit(1)


def get_ca_url(exhibitor_bootstrap_ca_url, bootstrap_url):
    if exhibitor_bootstrap_ca_url:
        print('Using `exhibitor_bootstrap_ca_url` config parameter.')
        return exhibitor_bootstrap_ca_url
    else:
        print('Inferring `exhibitor_bootstrap_ca_url` from `bootstrap_url`.')
        result = urlparse(bootstrap_url)

        if result.scheme == 'http':
            netloc = result.netloc.split(':', 1)  # strip port
            return f'https://{netloc[0]}:443'
        elif result.scheme == 'file':
            print('bootstrap url references a local file')
        else:
            print(f'bootstrap url is using an unsupported scheme: {result.scheme}')
        return ""


def test_connection(ca_url):
    s = socket.socket()
    s.settimeout(5)
    netloc = urlparse(ca_url).netloc.split(':', 1)
    host, port = netloc if len(netloc) == 2 else (netloc[0], '443')
    print(f'testing connection to {host}:{port}')
    try:
        s.connect((host, int(port)))
        print(f'connection to {host}:{port} successful')
        return True
    except Exception as e:
        print(f'could not connect to DC/OS bootstrap CA: {e}')
        return False
    finally:
        s.close()


def gen_tls_artifacts(ca_url, artifacts_path):
    """
    Contact the CA service to sign the generated CSR.
    Write the signed Exhibitor TLS artifacts to the file system.
    """
    # Fail early if IP detect script does not properly resolve yet.
    ip = invoke_detect_ip()

    psk_path = Path(PRESHAREDKEY_LOCATION)
    if psk_path.exists():
        psk = psk_path.read_text(encoding='ascii').strip()
        print(f'Using preshared key from location `{str(psk_path)}`')
    else:
        print(f'No preshared key found at location `{str(psk_path)}`')
        # Empty PSK results in any CSR being signed by the CA service.
        psk = ''

    server_entity = 'server'
    client_entity = 'client'

    print(f'Initiating {server_entity} end entity.')
    output = subprocess.check_output(
        args=[
            BOOTSTRAP_CA_BINARY,
            '--output-dir', EXHIBITOR_TLS_TMP_DIR,
            'init-entity', server_entity,
        ],
        stderr=subprocess.STDOUT,
    )
    print(output.decode())

    print(f'Initiating {client_entity} end entity.')
    output = subprocess.check_output(
        args=[
            BOOTSTRAP_CA_BINARY,
            '--output-dir', EXHIBITOR_TLS_TMP_DIR,
            'init-entity', client_entity,
        ],
        stderr=subprocess.STDOUT,
    )
    print(output.decode())

    print(f'Making CSR for {server_entity} with IP `{ip}`')
    output = subprocess.check_output(
        args=[
            BOOTSTRAP_CA_BINARY,
            'csr',
            server_entity,
            '--output-dir',
            EXHIBITOR_TLS_TMP_DIR,
            '--url',
            ca_url,
            '--ca',
            CSR_SERVICE_CERT_PATH,
            '--psk',
            psk,
            '--sans',
            f'{ip},localhost,127.0.0.1,exhibitor',
        ],
        stderr=subprocess.STDOUT,
    )

    print(output.decode())

    print(f'Making CSR for {client_entity} with IP `{ip}`')
    output = subprocess.check_output(
        args=[
            BOOTSTRAP_CA_BINARY,
            'csr',
            client_entity,
            '--output-dir',
            EXHIBITOR_TLS_TMP_DIR,
            '--url',
            ca_url,
            '--ca',
            CSR_SERVICE_CERT_PATH,
            '--psk',
            psk,
            '--sans',
            f'{ip},localhost,127.0.0.1,exhibitor',
        ],
        stderr=subprocess.STDOUT,
    )

    print(output.decode())

    print(f'Writing TLS artifacts to {artifacts_path}')
    output = subprocess.check_output(
        args=[
            BOOTSTRAP_CA_BINARY,
            'create-exhibitor-artifacts',
            '--output-dir',
            EXHIBITOR_TLS_TMP_DIR,
            '--ca',
            CSR_SERVICE_CERT_PATH,
            '--client-entity',
            client_entity,
            '--server-entity',
            server_entity,
            '--artifacts-directory',
            f'{artifacts_path}',
        ],
        stderr=subprocess.STDOUT,
    )

    print(output.decode())


def _fail_and_calculate_status(message):
    exit_status = 1 if os.environ.get('EXHIBITOR_TLS_REQUIRED') == 'true' else 0
    if exit_status:
        print('exhibitor TLS bootstrap failed, but is required')
    else:
        print('exhibitor TLS bootstrap failed, launching in insecure mode')
    print(message)
    sys.exit(exit_status)


def _remove_artifact_dir():
    p = Path(TLS_ARTIFACT_LOCATION)
    if p.is_dir():
        for f in p.iterdir():
            f.unlink()
        p.rmdir()


def main():
    if os.environ.get('EXHIBITOR_TLS_ENABLED', 'false') == 'false':
        _fail_and_calculate_status('exhibitor TLS is disabled')

    if Path(TLS_ARTIFACT_LOCATION).exists():
        return

    if not Path(CSR_SERVICE_CERT_PATH).exists():
        _fail_and_calculate_status('root CA certificate does not exist')

    exhibitor_bootstrap_ca_url = os.environ['EXHIBITOR_BOOTSTRAP_CA_URL']
    bootstrap_url = os.environ['BOOTSTRAP_URL']

    ca_url = get_ca_url(exhibitor_bootstrap_ca_url, bootstrap_url)
    if not ca_url:
        _fail_and_calculate_status('could not determine ca service url')

    if not test_connection(ca_url):
        _fail_and_calculate_status('connection failed')

    print('Bootstrapping exhibitor TLS')

    try:
        gen_tls_artifacts(ca_url, Path(TLS_ARTIFACT_LOCATION))
    except subprocess.CalledProcessError as cpe:
        # Clean up any partially written artifacts
        _remove_artifact_dir()
        _fail_and_calculate_status(
            f'error generating artifacts code: {cpe.returncode} stdout: {cpe.stdout} stderr: {cpe.stderr}'
        )

    # remove file from temporary location
    Path(CSR_SERVICE_CERT_PATH).unlink()


if __name__ == '__main__':
    main()
    # Always flush stdout buffer when exiting the script
    sys.stdout.flush()
