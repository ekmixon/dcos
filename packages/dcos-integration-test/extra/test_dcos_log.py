import json
import logging
import re
import uuid

from typing import Any, Optional

import pytest
import requests
import retrying

from dcos_test_utils.dcos_api import DcosApiSession


__maintainer__ = 'mnaboka'
__contact__ = 'dcos-cluster-ops@mesosphere.io'


NEW_ENTRY_PATTERN = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}: "

log = logging.getLogger(__name__)


def skip_test_if_dcos_journald_log_disabled(dcos_api_session: DcosApiSession) -> None:
    response = dcos_api_session.get('/dcos-metadata/ui-config.json').json()
    try:
        strategy = response['uiConfiguration']['plugins']['mesos']['logging-strategy']
    except Exception:
        log.exception('Unable to find logging strategy')
        raise
    if not strategy.startswith('journald'):
        pytest.skip('Skipping a test since journald logging is disabled')


def validate_json_entry(entry: dict) -> None:
    required_fields = {'fields', 'cursor', 'monotonic_timestamp', 'realtime_timestamp'}

    assert (
        set(entry.keys()) <= required_fields
    ), f"Entry didn't have all required fields. Entry fields: {entry}, required fields:{required_fields}"


    assert entry['fields'], f'`fields` cannot be empty dict. Got {entry}'


def validate_sse_entry(entry: str) -> None:
    assert entry, f'Expect at least one line. Got {entry}'
    entry_json = json.loads(entry.lstrip('data: '))
    validate_json_entry(entry_json)


def check_response_ok(response: requests.models.Response, headers: dict) -> None:
    assert (
        response.ok
    ), f'Request {response.url} returned response code {response.status_code}'

    for name, value in headers.items():
        assert (
            response.headers.get(name) == value
        ), f'Request {response.url} header {name} must be {value}. All headers {response.headers}'


def test_log_text(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v1/range/?limit=10', node=node)
        check_response_ok(response, {'Content-Type': 'text/plain'})

        # expect 10 entries
        logs = response.content.decode()
        entries_count = len(re.findall(NEW_ENTRY_PATTERN, logs))
        assert (
            entries_count == 10
        ), f'Expect 10 log entries. Got {entries_count}. All lines {logs}'


def test_log_json(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v1/range/?limit=1', node=node, headers={'Accept': 'application/json'})
        check_response_ok(response, {'Content-Type': 'application/json'})
        validate_json_entry(response.json())


def test_log_server_sent_events(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v1/range/?limit=1', node=node, headers={'Accept': 'text/event-stream'})
        check_response_ok(response, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
        validate_sse_entry(response.text)


def test_stream(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v1/stream/?skip_prev=1', node=node, stream=True,
                                             headers={'Accept': 'text/event-stream'})
        check_response_ok(response, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
        lines = response.iter_lines()
        sse_id = next(lines)
        assert sse_id, f'First line must be id. Got {sse_id}'
        data = next(lines).decode('utf-8', 'ignore')
        validate_sse_entry(data)


def test_log_proxy(dcos_api_session: DcosApiSession) -> None:
    r = dcos_api_session.get('/mesos/master/slaves')
    check_response_ok(r, {})

    data = r.json()
    slaves_ids = sorted(x['id'] for x in data['slaves'] if x['hostname'] in dcos_api_session.all_slaves)

    for slave_id in slaves_ids:
        response = dcos_api_session.get(
            f'/system/v1/agent/{slave_id}/logs/v1/range/?skip_prev=10&limit=10'
        )

        check_response_ok(response, {'Content-Type': 'text/plain'})
        lines = list(filter(lambda x: x != '', response.text.split('\n')))
        assert (
            len(lines) == 10
        ), f'Expect 10 log entries. Got {len(lines)}. All lines {lines}'


def test_task_logs(dcos_api_session: DcosApiSession) -> None:
    skip_test_if_dcos_journald_log_disabled(dcos_api_session)
    test_uuid = uuid.uuid4().hex

    task_id = f"integration-test-task-logs-{test_uuid}"

    task_definition = {
        "id": f"/{task_id}",
        "cpus": 0.1,
        "instances": 1,
        "mem": 128,
        "cmd": "echo STDOUT_LOG; echo STDERR_LOG >&2;sleep 999",
    }


    with dcos_api_session.marathon.deploy_and_cleanup(task_definition, check_health=False):
        url = get_task_url(dcos_api_session, task_id)
        check_response(
            'STDOUT_LOG',
            lambda: dcos_api_session.get(f'{url}?filter=STREAM:STDOUT'),
        )

        check_response(
            'STDERR_LOG',
            lambda: dcos_api_session.get(f'{url}?filter=STREAM:STDERR'),
        )


        stream_url = get_task_url(dcos_api_session, task_id, stream=True)
        response = dcos_api_session.get(stream_url, stream=True, headers={'Accept': 'text/event-stream'})
        check_response_ok(response, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
        lines = response.iter_lines()
        sse_id = next(lines)
        assert sse_id, f'First line must be id. Got {sse_id}'
        data = next(lines).decode('utf-8', 'ignore')
        validate_sse_entry(data)


def test_pod_logs(dcos_api_session: DcosApiSession) -> None:
    skip_test_if_dcos_journald_log_disabled(dcos_api_session)
    test_uuid = uuid.uuid4().hex

    pod_id = f'integration-test-pod-logs-{test_uuid}'

    pod_definition = {
        'id': f'/{pod_id}',
        'scaling': {'kind': 'fixed', 'instances': 1},
        'environment': {'PING': 'PONG'},
        'containers': [
            {
                'name': 'sleep1',
                'exec': {
                    'command': {
                        'shell': 'echo $PING > foo;echo STDOUT_LOG;echo STDERR_LOG >&2;sleep 10000'
                    }
                },
                'resources': {'cpus': 0.1, 'mem': 32},
                'healthcheck': {
                    'command': {'shell': 'test $PING = `cat foo`'}
                },
            }
        ],
        'networks': [{'mode': 'host'}],
    }


    with dcos_api_session.marathon.deploy_pod_and_cleanup(pod_definition):
        url = get_task_url(dcos_api_session, pod_id)
        container_id = url.split('/')[-1]

        check_response(
            'STDOUT_LOG',
            lambda: dcos_api_session.get(f'{url}?filter=STREAM:STDOUT'),
        )

        check_response(
            'STDERR_LOG',
            lambda: dcos_api_session.get(f'{url}?filter=STREAM:STDERR'),
        )


        response = dcos_api_session.get(
            f'{url}/download', query='limit=10&postfix=stdout'
        )

        log_file_name = f'task-{container_id}-stdout.log.gz'
        check_response_ok(
            response,
            {'Content-Disposition': f'attachment; filename={log_file_name}'},
        )


@retrying.retry(wait_fixed=1000, stop_max_delay=3000)
def check_response(content: str, get_response: Any) -> None:
    response = get_response()
    check_response_ok(response, {})
    assert (
        content in response.text
    ), f'Missing {content} in response {response.text}'


def get_task_url(dcos_api_session: DcosApiSession, task_name: str, stream: bool=False) -> str:
    """ The function returns a logging URL for a given task

    :param dcos_api_session: dcos_api_session fixture
    :param task_name: task name
    :param stream: use range or stream endpoint
    :return: url to get the logs for a task
    """
    state_response = dcos_api_session.get('/mesos/state')
    check_response_ok(state_response, {})

    framework_id = None
    executor_id = None
    slave_id = None
    container_id = None

    state_response_json = state_response.json()
    assert (
        'frameworks' in state_response_json
    ), f'Missing field `framework` in {state_response_json}'

    assert isinstance(
        state_response_json['frameworks'], list
    ), f'`framework` must be list. Got {state_response_json}'


    for framework in state_response_json['frameworks']:
        assert (
            'name' in framework
        ), f'Missing field `name` in `frameworks`. Got {state_response_json}'

        # search for marathon framework
        if framework['name'] != 'marathon':
            continue

        assert (
            'tasks' in framework
        ), f'Missing field `tasks`. Got {state_response_json}'

        assert isinstance(
            framework['tasks'], list
        ), f'`tasks` must be list. Got {state_response_json}'

        for task in framework['tasks']:
            assert 'id' in task, f'Missing field `id` in task. Got {state_response_json}'
            if not task['id'].startswith(task_name):
                continue

            assert (
                'framework_id' in task
            ), f'Missing `framework_id` in task. Got {state_response_json}'

            assert (
                'executor_id' in task
            ), f'Missing `executor_id` in task. Got {state_response_json}'

            assert 'id' in task, f'Missing `id` in task. Got {state_response_json}'
            assert (
                'slave_id' in task
            ), f'Missing `slave_id` in task. Got {state_response_json}'


            framework_id = task['framework_id']
            # if task['executor_id'] is empty, we should use task['id']
            executor_id = task['executor_id']
            if not executor_id:
                executor_id = task['id']
            slave_id = task['slave_id']

            assert task['statuses'], f'Invalid field `statuses`. Got {state_response_json}'
            statuses = task['statuses']
            assert isinstance(
                statuses, list
            ), f'Invalid field `statuses`. Got {state_response_json}'

            assert (
                len(statuses) == 1
            ), f'Must have only one status TASK_RUNNING. Got {state_response_json}'

            status = statuses[0]
            assert status[
                'container_status'
            ], f'Invalid field `container_status`. Got {state_response_json}'

            container_status = status['container_status']
            assert container_status[
                'container_id'
            ], f'Invalid field `container_id`. Got {state_response_json}'

            container_id_field = container_status['container_id']

            # traverse nested container_id fields
            container_ids = [container_id_field['value']]
            while 'parent' in container_id_field:
                container_id_field = container_id_field['parent']
                container_ids.append(container_id_field['value'])

            container_id = '.'.join(reversed(container_ids))
            assert container_id

    # validate all required fields
    assert slave_id, 'Missing slave_id'
    assert framework_id, 'Missing framework_id'
    assert executor_id, 'Missing executor_id'
    assert container_id, 'Missing container_id'

    endpoint_type = 'stream' if stream else 'range'
    return f'/system/v1/agent/{slave_id}/logs/v1/{endpoint_type}/framework/{framework_id}/executor/{executor_id}/container/{container_id}'


def validate_journald_cursor(c: Any, cursor_regexp: Optional[bytes] = None) -> None:
    if not cursor_regexp:
        cursor_regexp = b'^id: s=[a-f0-9]+;i=[a-f0-9]+;b=[a-f0-9]+;'
        cursor_regexp += b'm=[a-f0-9]+;t=[a-f0-9]+;x=[a-f0-9]+$'

    p = re.compile(cursor_regexp)
    assert p.match(c), f"Cursor {c} does not match regexp {cursor_regexp.decode()}"


def test_log_v2_text(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v2/component?limit=10', node=node)
        check_response_ok(response, {'Content-Type': 'text/plain'})

        # expect 10 entries
        logs = response.content.decode()
        entries_count = len(re.findall(NEW_ENTRY_PATTERN, logs))
        assert (
            entries_count == 10
        ), f'Expect 10 log entries. Got {entries_count}. All lines {logs}'


def test_log_v2_server_sent_events(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get(
            '/v2/component?limit=1', node=node, headers={'Accept': 'text/event-stream'}, stream=True)
        check_response_ok(response, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
        lines = response.iter_lines()
        sse_id = next(lines)
        validate_journald_cursor(sse_id)
        data = next(lines).decode('utf-8', 'ignore')
        validate_sse_entry(data)


def test_log_v2_stream(dcos_api_session: DcosApiSession) -> None:
    for node in dcos_api_session.masters + dcos_api_session.all_slaves:
        response = dcos_api_session.logs.get('/v2/component?skip=-1', node=node, stream=True,
                                             headers={'Accept': 'text/event-stream'})
        check_response_ok(response, {'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache'})
        lines = response.iter_lines()
        sse_id = next(lines)
        validate_journald_cursor(sse_id)
        data = next(lines).decode('utf-8', 'ignore')
        validate_sse_entry(data)


def test_log_v2_proxy(dcos_api_session: DcosApiSession) -> None:
    r = dcos_api_session.get('/mesos/master/slaves')
    check_response_ok(r, {})

    data = r.json()
    slaves_ids = sorted(x['id'] for x in data['slaves'] if x['hostname'] in dcos_api_session.all_slaves)

    for slave_id in slaves_ids:
        response = dcos_api_session.get(
            f'/system/v1/agent/{slave_id}/logs/v2/component?skip=-10&limit=10'
        )

        check_response_ok(response, {'Content-Type': 'text/plain'})
        lines = list(filter(lambda x: x != '', response.text.split('\n')))
        assert (
            len(lines) == 10
        ), f'Expect 10 log entries. Got {len(lines)}. All lines {lines}'


def test_log_v2_task_logs(dcos_api_session: DcosApiSession) -> None:
    test_uuid = uuid.uuid4().hex

    task_id = f"integration-test-task-logs-{test_uuid}"

    task_definition = {
        "id": f"/{task_id}",
        "cpus": 0.1,
        "instances": 1,
        "mem": 128,
        "healthChecks": [
            {
                "protocol": "COMMAND",
                "command": {
                    "value": "grep -q STDOUT_LOG stdout;grep -q STDERR_LOG stderr"
                },
            }
        ],
        "cmd": "echo STDOUT_LOG; echo STDERR_LOG >&2;sleep 999",
    }


    with dcos_api_session.marathon.deploy_and_cleanup(task_definition, check_health=True):
        check_response(
            'STDOUT_LOG',
            lambda: dcos_api_session.logs.get(
                f'/v2/task/{task_id}/file/stdout'
            ),
        )

        check_response(
            'STDERR_LOG',
            lambda: dcos_api_session.logs.get(
                f'/v2/task/{task_id}/file/stderr'
            ),
        )

        _assert_files_in_browse_response(dcos_api_session, task_id, ['stdout', 'stderr'])
        _assert_can_download_files(dcos_api_session, task_id, ['stdout', 'stderr'])


def test_log_v2_pod_logs(dcos_api_session: DcosApiSession) -> None:
    test_uuid = uuid.uuid4().hex

    pod_id = f'integration-test-pod-logs-{test_uuid}'

    pod_definition = {
        'id': f'/{pod_id}',
        'scaling': {'kind': 'fixed', 'instances': 1},
        'environment': {'PING': 'PONG'},
        'containers': [
            {
                'name': 'sleep1',
                'exec': {
                    'command': {
                        'shell': 'echo $PING > foo;echo STDOUT_LOG;echo STDERR_LOG >&2;sleep 10000'
                    }
                },
                'resources': {'cpus': 0.1, 'mem': 32},
                'healthcheck': {
                    'command': {'shell': 'test $PING = `cat foo`'}
                },
            }
        ],
        'networks': [{'mode': 'host'}],
    }


    with dcos_api_session.marathon.deploy_pod_and_cleanup(pod_definition):
        check_response('STDOUT_LOG', lambda: dcos_api_session.logs.get('/v2/task/sleep1'))
        check_response('STDERR_LOG', lambda: dcos_api_session.logs.get('/v2/task/sleep1/file/stderr'))
        _assert_files_in_browse_response(dcos_api_session, pod_id, ['stdout', 'stderr', 'foo'])
        _assert_can_download_files(dcos_api_session, pod_id, ['stdout', 'stderr', 'foo'])


def test_log_v2_api(dcos_api_session: DcosApiSession) -> None:
    test_uuid = uuid.uuid4().hex

    task_id = f"integration-test-task-logs-{test_uuid}"

    task_definition = {
        "id": f"/{task_id}",
        "cpus": 0.1,
        "instances": 1,
        "mem": 128,
        "healthChecks": [
            {"protocol": "COMMAND", "command": {"value": "test -f test"}}
        ],
        "cmd": "echo \"one\ntwo\nthree\nfour\nfive\n\">test;sleep 9999",
    }


    with dcos_api_session.marathon.deploy_and_cleanup(task_definition, check_health=True):
        # skip 2 entries from the beggining
        response = dcos_api_session.logs.get(f'/v2/task/{task_id}/file/test?skip=2')
        check_response_ok(response, {})
        assert response.text == "three\nfour\nfive\n"

        # move to the end of file and read 2 last LINE_SIZE
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?cursor=END&skip=-2'
        )

        check_response_ok(response, {})
        assert response.text == "four\nfive\n"

        # move three lines from the top and limit to one entry
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?skip=3&limit=1'
        )

        check_response_ok(response, {})
        assert response.text == "four\n"

        # set cursor to 7 (bytes) which the second word and skip 1 lines
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?cursor=7&skip=1'
        )

        check_response_ok(response, {})
        assert response.text == "four\nfive\n"

        # set cursor to 7 (bytes) which the second word and skip -1 lines and limit 1
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?cursor=7&skip=-1&limit=1'
        )

        check_response_ok(response, {})
        assert response.text == "two\n"

        # validate the bug is fixed https://jira.mesosphere.com/browse/DCOS_OSS-1995
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?cursor=END&skip=-5'
        )

        check_response_ok(response, {})
        assert response.text == "one\ntwo\nthree\nfour\nfive\n"

        # make sure if 'Last-Event-ID' header is passed, other get parameters are ignored
        # https://jira.mesosphere.com/browse/DCOS_OSS-2292
        # number 7 used in 'Last-Event-ID' header points to a second word.
        response = dcos_api_session.logs.get(
            f'/v2/task/{task_id}/file/test?cursor=END&skip=-1',
            headers={'Last-Event-ID': '7'},
        )

        check_response_ok(response, {})
        assert response.text == "three\nfour\nfive\n"


def _assert_files_in_browse_response(dcos_api_session: DcosApiSession, task: str, expected_files: Any) -> None:
    response = dcos_api_session.logs.get(f'/v2/task/{task}/browse')
    check_response_ok(response, {})

    expected_fields = ['gid', 'mode', 'mtime', 'nlink', 'path', 'size', 'uid']
    data = response.json()
    files = []
    for item in data:
        for field in expected_fields:
            assert field in item, f'Field {field} must be in response. Item {item}'
        _file = item['path'].split('/')[-1]
        files += [_file]

    for expected_file in expected_files:
        assert expected_file in files, f'Expecting file {expected_file} in {files}'


def _assert_can_download_files(dcos_api_session: DcosApiSession, task: str, expected_files: Any) -> None:
    for expected_file in expected_files:
        response = dcos_api_session.logs.get(
            f'/v2/task/{task}/file/{expected_file}/download'
        )

        check_response_ok(
            response,
            {
                'Content-Type': 'application/octet-stream',
                'Content-Disposition': f'attachment; filename={expected_file}',
            },
        )

        assert response.text
