import os
import sys
import logging
from logging.handlers import RotatingFileHandler

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, LogData
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.util.types import Attributes
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry._logs import set_logger_provider

class CustomLogProcessor(BatchLogRecordProcessor):
    def __init__(self, exporter):
        super().__init__(exporter)
        self.attributes = {
            "service_process_name": "coordinator",
            "organization_id": int(os.environ.get('ORGANIZATION_ID', '1')),
            "service_name": os.environ.get('SERVICE_NAME', 'service_name'),
            "account_id": int(os.environ.get('ACCOUNT_ID', '2')),
            "db_instance_id": int(os.environ.get('DATABASE_INSTANCE_ID', '1234')),
            "instance_key": os.environ.get('INSTANCE_KEY', 'instance_key')
        }

    def emit(self, log_data: LogData):
        """
        Emit a log record.

        Override to add environment-specific attributes to the log record.
        """
        attributes: Attributes = log_data.log_record.attributes.copy()
        new_attributes: Attributes = {}

        # Map attributes to new names
        for k, v in attributes.items():
            if k == 'code.filepath':
                new_attributes['source_file'] = v
            elif k == 'code.lineno':
                new_attributes['source_line'] = v
            elif k == 'code.function':
                new_attributes['source_function'] = v
            else:
                new_attributes[k] = v

        # Add default attributes
        new_attributes.update(self.attributes)
        log_data.log_record.attributes = BoundedAttributes(attributes=new_attributes)
        super().emit(log_data)


def init_otel_logging(endpoint: str):
    """Initialize OpenTelemetry logging with the specified endpoint."""

    # Set up OpenTelemetry tracer provider
    trace.set_tracer_provider(TracerProvider())
    trace.get_tracer_provider().add_span_processor(
        BatchSpanProcessor(ConsoleSpanExporter())
    )

    # Create logger provider
    logger_provider = LoggerProvider()
    exporter = OTLPLogExporter(endpoint=endpoint)
    logger_provider.add_log_record_processor(CustomLogProcessor(exporter))
    set_logger_provider(logger_provider)

    handler = LoggingHandler(level=logging.INFO, logger_provider=logger_provider)

    return handler

def init_logging(
        otel_config: dict,
        log_path: str,
        debug: bool = False,
        logger_name: str = 'springtail',
        log_rotation_size : int = 0,
        log_rotation_count : int = 10
    ) -> None:
    """
    Initialize logging for the coordinator.
    """
    # Create a custom logger
    logger = logging.getLogger(logger_name)
    if debug:
        logger.setLevel(logging.DEBUG)  # Capture all logs at the logger level
    else:
        logger.setLevel(logging.INFO)

    # **Handler 1: Console Logging (DEBUG and below)**
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)  # Show debug logs in console
    console_format = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)-5s [%(filename)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # **Handler 2: OTLP (INFO and above)**
    if 'enabled' in otel_config and otel_config['enabled'] and 'host' in otel_config and 'port' in otel_config:
        otel_handler = init_otel_logging(f"http://{otel_config.get('host', 'localhost')}:{otel_config.get('port', '4318')}/v1/logs")
        otel_handler.setLevel(logging.INFO)  # Send only INFO and above to OTEL
        logger.addHandler(otel_handler)

    # **Handler 3: File Logging (DEBUG and above)**
    if log_rotation_size > 0:
        file_handler = RotatingFileHandler(os.path.join(log_path, 'coordinator.log'), maxBytes=log_rotation_size, backupCount=log_rotation_count)
    else:
        file_handler = logging.FileHandler(os.path.join(log_path, 'coordinator.log'))
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(console_format)
    logger.addHandler(file_handler)

if __name__ == "__main__":
    init_logging({'enabled': True, 'host': 'localhost', 'port': '4318'}, '/tmp', True)
    logger = logging.getLogger('springtail')
    logger.info("This is an info message", extra={'db_id': 11})
    logger.debug("This is a debug message")
    logger.error("This is an error message")