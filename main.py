import argparse
import logging
import re
import subprocess
import sys
import threading
from datetime import datetime, timedelta

from kubernetes import client, config
from kubernetes.stream import stream

from exeptions import InvalidJwtSource


logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)

logger = logging.getLogger('kubernetes')

pod_names_lock = threading.Lock()


class PodNames:

    pod_name_list = []
    last_updated = None

    @classmethod
    def refresh_pod_list(cls, kube_client, namespace):
        pod_list = kube_client.list_namespaced_pod(namespace=namespace).items
        with pod_names_lock:
            cls.pod_name_list.clear()
            for pod in pod_list:
                cls.pod_name_list.append(pod.metadata.name)
            cls.last_updated = datetime.now()

    @classmethod
    def get_pod_names(cls, kube_client, namespace):
        f_update_names = (datetime.now() - cls.last_updated) > timedelta(seconds=10) if cls.last_updated else True
        if f_update_names:
            cls.refresh_pod_list(kube_client, namespace)
        return cls.pod_name_list


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='KubeConnect',
    )
    parser.add_argument('--port', '-p', type=int, default=5000)
    parser.add_argument('--namespace', '-n', type=str)
    parser.add_argument('--jwt-source-pod', type=str, default='')
    parser.add_argument('--jwt-source-container', type=str, default='')
    parser.add_argument('--env-file', '-e', type=str, default='.env')
    parser.add_argument('--pods-file', '-f', type=str, default='pods')
    return parser


def get_jwt_source_pod(source_pod_search_name, namespace):
    pod_name_list = PodNames.get_pod_names(kube_client, namespace)
    for pod_name in pod_name_list:
        if pod_name.startswith(source_pod_search_name):
            source_pod_name = pod_name
            break
    else:
        raise InvalidJwtSource()
    return source_pod_name


def get_jwt_token_env(kube_client, source_pod_search_name, source_container, namespace):
    source_pod_name = get_jwt_source_pod(source_pod_search_name, namespace)
    jwt_response = stream(
        kube_client.connect_post_namespaced_pod_exec,
        name=source_pod_name,
        container=source_container,
        namespace=namespace,
        command=['/bin/sh', '-c', 'env | grep JWT'],
        stderr=True, stdin=False,
        stdout=True, tty=False,
    )
    return jwt_response.strip()


def modify_env_file(env_file_path, jwt_token_env):
    with open(env_file_path, 'r') as env_file:
        env_file_data = env_file.read()

    env_jwt_re = re.compile('JWT_SECRET=.*', re.MULTILINE)

    if not env_jwt_re.search(env_file_data):
        env_file_data += jwt_token_env
    else:
        env_file_data = env_jwt_re.sub(jwt_token_env, env_file_data)

    with open(env_file_path, 'w') as env_file:
        env_file.write(env_file_data)


def update_env_file(env_file_path, source_pod_search_name, source_container, namespace, kube_client):
    jwt_token_env = get_jwt_token_env(kube_client, source_pod_search_name, source_container, namespace)

    modify_env_file(env_file_path, jwt_token_env)


def get_pods_data(pods_file_path):
    with open(pods_file_path, 'r') as pods_file:
        pods_data = pods_file.readlines()
    return pods_data


def run_port_forwarding(pod_short_name, namespace, port_local, port_pod, kube_client):
    while True:
        pod_name_list = PodNames.get_pod_names(kube_client, namespace)
        for pod_name in pod_name_list:
            if pod_name.startswith(pod_short_name):
                pod_full_name = pod_name
                break
        else:
            logger.error('No pod found for %s', pod_short_name)
            continue
        try:
            logger.info('Running port forwarding for %s', pod_full_name)
            port_forward_sp = subprocess.run(
                ['kubectl', 'port-forward', '--address', '0.0.0.0', '-n', namespace, pod_full_name,
                 f'{port_local}:{port_pod}'],
                capture_output=True,
            )
        except Exception:
            logger.exception('Failed to forward %s', pod_full_name)
        if port_forward_sp.stdout:
            logger.error(port_forward_sp.stdout)


def run_port_forwarding_for_all(pods_file_path, kube_client, namespace):
    pods_data = get_pods_data(pods_file_path)

    threads = []
    for pod_line in pods_data:
        pod_line = pod_line.strip()
        pod_short_name, port_local, port_pod = pod_line.split(' ')
        thread = threading.Thread(
            target=run_port_forwarding,
            kwargs={
                'pod_short_name': pod_short_name,
                'namespace': namespace,
                'port_local': port_local,
                'port_pod': port_pod,
                'kube_client': kube_client,
            },
            daemon=True
        )
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()

    namespace = args.namespace
    source_pod_search_name = args.jwt_source_pod
    source_container = args.jwt_source_pod
    env_file_path = args.env_file
    pods_file_path = args.pods_file

    config.load_kube_config()

    kube_client = client.CoreV1Api()

    update_env_file(env_file_path, source_pod_search_name, source_container, namespace, kube_client)
    run_port_forwarding_for_all(pods_file_path, kube_client, namespace)
