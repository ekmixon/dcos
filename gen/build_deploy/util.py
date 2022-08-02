import os
import os.path
import shutil
from datetime import datetime
from subprocess import check_output

from pkgpanda.util import write_json, write_string

dcos_image_commit = os.getenv('DCOS_IMAGE_COMMIT', None)

if dcos_image_commit is None:
    dcos_image_commit = check_output(['git', 'rev-parse', 'HEAD']).decode('utf-8').strip()

if dcos_image_commit is None:
    raise ValueError("Unable to set dcos_image_commit from environment or git.")

template_generation_date = str(datetime.utcnow())


def try_makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


def copy_makedirs(src, dest):
    try_makedirs(os.path.dirname(dest))
    shutil.copy(src, dest)


def do_bundle_onprem(gen_out, output_dir):
    # We are only being called via dcos_generate_config.sh with an output_dir
    assert output_dir is not None
    assert output_dir
    assert output_dir[-1] != '/'
    output_dir = f'{output_dir}/'

    # Copy generated artifacts
    for filename in gen_out.channel_artifacts + gen_out.stable_artifacts:
        copy_makedirs(filename, output_dir + filename)

    # Write an index of the cluster packages
    write_json(f'{output_dir}cluster-package-info.json', gen_out.cluster_packages)

    # Write the bootstrap id
    write_string(
        f'{output_dir}bootstrap.latest', gen_out.arguments['bootstrap_id']
    )


    # Write cluster package list ID
    write_string(
        f'{output_dir}cluster-package-list.latest',
        gen_out.arguments['cluster_package_list_id'],
    )


def variant_str(variant):
    """Return a string representation of variant."""
    return '' if variant is None else variant


def variant_name(variant):
    """Return a human-readable string representation of variant."""
    return '<default>' if variant is None else variant


def variant_prefix(variant):
    """Return a filename prefix for variant."""
    return '' if variant is None else f'{variant}.'
