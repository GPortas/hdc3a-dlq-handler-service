"""
This module defines a StompInteractor, which is an abstract class intended
to define common behavior for stomp-implemented MQ components.
"""
import logging
from abc import ABC, abstractmethod

import stomp
from tenacity import stop_after_attempt, retry_if_exception_type, retry, before_log

from app.common.domain.mq.exceptions.mq_connection_exception import MqConnectionException
from app.common.infrastructure.mq.mq_connection_params import MqConnectionParams


class StompInteractor(ABC):
    __STOMP_CONN_HEARTBEATS_MS = 40000
    __STOMP_CONN_MAX_RETRIES = 2
    __STOMP_CONN_TIMEOUT_MS = 5000

    def __init__(self) -> None:
        self._logger = logging.getLogger()

    @retry(
        stop=stop_after_attempt(__STOMP_CONN_MAX_RETRIES),
        retry=retry_if_exception_type(MqConnectionException),
        reraise=True,
        before=before_log(logging.getLogger(), logging.INFO)
    )
    def _create_mq_connection(self) -> stomp.Connection:
        """
        Creates a stomp.Connection to MQ.
        """
        mq_connection_params = self._get_mq_connection_params()

        mq_host = mq_connection_params.mq_host
        mq_port = mq_connection_params.mq_port
        mq_ssl_enabled = mq_connection_params.mq_ssl_enabled
        mq_user = mq_connection_params.mq_user
        mq_password = mq_connection_params.mq_password

        self._logger.debug(
            "Creating MQ connection... Host: " + mq_host + ", port: " + mq_port + ", SSL enabled: " + mq_ssl_enabled
        )
        try:
            connection = stomp.Connection(
                host_and_ports=[(mq_host, mq_port)],
                heartbeats=(self.__STOMP_CONN_HEARTBEATS_MS, self.__STOMP_CONN_HEARTBEATS_MS),
                keepalive=True
            )

            if mq_ssl_enabled == 'True':
                connection.set_ssl([(mq_host, mq_port)])

            connection.connect(
                mq_user,
                mq_password,
                wait=True,
                timeout=self.__STOMP_CONN_TIMEOUT_MS
            )

            self._logger.debug(
                "MQ connection created. Host: " + mq_host + ", port: " + mq_port + ", SSL enabled: " + mq_ssl_enabled
            )
            return connection

        except Exception as e:
            self._logger.error(str(e))
            raise MqConnectionException(
                queue_host=mq_host,
                queue_port=mq_port,
                reason=str(e)
            )

    @abstractmethod
    def _get_mq_connection_params(self) -> MqConnectionParams:
        """
        Retrieves the MQ connection params necessary for creating the connection
        """
