# Copyright (C) Mesosphere, Inc. See LICENSE file for details.

"""gRPC mock endpoint"""

import logging
import time
from concurrent import futures

import grpc
from google.rpc import code_pb2, status_pb2
from grpc_status import rpc_status

from mocker.endpoints import generic, grpc_endpoint_pb2, grpc_endpoint_pb2_grpc

# pylint: disable=C0103
log = logging.getLogger(__name__)


class MockServiceServicer(grpc_endpoint_pb2_grpc.MockServiceServicer):
    def __init__(self, get_context_data_f):
        super().__init__()
        self._get_context_data_f = get_context_data_f

    def UnaryDoSomething(self, request, context):
        if self._get_context_data_f("always_bork"):
            log.debug("gRPC server is borking the unary request")
            status = status_pb2.Status(
                code=code_pb2.FAILED_PRECONDITION,
                message="request borked per request",
            )
            context.abort_with_status(rpc_status.to_status(status))
        elif self._get_context_data_f("always_stall"):
            stall_time = self._get_context_data_f("stall_time")
            log.debug(
                f"gRPC server is stalling the unary request for `{stall_time}` seconds"
            )

            time.sleep(stall_time)

        return grpc_endpoint_pb2.StringMessage(message=f"received: {request.message}")

    def ServerSteramDoSomething(self, request, context):
        for i in request.messageIDs:
            yield grpc_endpoint_pb2.IntMessage(messageID=i)

    def ClientStreamDoSomething(self, request_iterator, context):
        receivedIDs = [message.messageID for message in request_iterator]

        return grpc_endpoint_pb2.IntCollectionMessage(messageIDs=receivedIDs)


class GRPCEndpoint(generic.Endpoint):
    """gRPC server mock endpoint"""
    def __init__(self, port, ip='', keyfile=None, certfile=None, cafile=None):
        """Initialize new GRPCEndpoint object

        Args:
            port (int): tcp port that grpc server will listen on
            ip (str): ip address that grpc server will listen on, by default
                listen on all addresses
            keyfile(str): path to the key of the certificate
            certfile(str): path to the certificate to be used by server (if any)
            cafile(str): path to the CA certificate (if any)
        """
        if certfile is not None and keyfile is not None and cafile is not None:
            self._is_tls = True
            endpoint_id = f"grpcs://{ip}:{port}"
            log.debug("gRPC server is runs in TLS mode")
        else:
            self._is_tls = False
            endpoint_id = f"grpc://{ip}:{port}"
            log.debug("gRPC server is runs in plaintext mode")
        super().__init__(endpoint_id)

        self._context.data['listen_ip'] = ip
        self._context.data['listen_port'] = port
        self._context.data['certfile'] = certfile
        self._context.data['keyfile'] = keyfile
        self._context.data['cafile'] = cafile

    @staticmethod
    def _load_credential_from_file(filepath):
        with open(filepath, 'rb') as f:
            return f.read()

    def get_context_data(self, key):
        with self._context.lock:
            return self._context.data[key]

    def start(self):
        log.debug(f"gRPC server {self.id} is starting")
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_endpoint_pb2_grpc.add_MockServiceServicer_to_server(
            MockServiceServicer(self.get_context_data),
            self._server,
        )
        if self._is_tls:
            server_credentials = grpc.ssl_server_credentials(
                private_key_certificate_chain_pairs=[(
                    self._load_credential_from_file(self._context.data['keyfile']),
                    self._load_credential_from_file(self._context.data['certfile']),
                )],
                root_certificates=self._load_credential_from_file(
                    self._context.data['cafile']),
                require_client_auth=True,
            )
            self._server.add_secure_port(
                f"{self._context.data['listen_ip']}:{self._context.data['listen_port']}",
                server_credentials,
            )

        else:
            self._server.add_insecure_port(
                f"{self._context.data['listen_ip']}:{self._context.data['listen_port']}"
            )

        self._server.start()

    def stop(self):
        self._server.stop()
        self._server.wait_for_termination()
