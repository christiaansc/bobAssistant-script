# simple_logger.py
import logging
import sys

class Logger:
    """
    Clase simple para configurar y manejar logs con fecha y hora.
    """
    # Crear un logger específico para esta clase
    logger = logging.getLogger("Logger")
    logger.setLevel(logging.DEBUG)  # Captura todos los niveles de log

    # Verificar si ya se han añadido handlers para evitar duplicaciones
    if not logger.hasHandlers():
        # Definir el formato de los logs
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # Crear un manejador para stdout (INFO y superiores)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(formatter)

        # Crear un manejador para stderr (WARNING y superiores)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.WARNING)
        stderr_handler.setFormatter(formatter)

        # Añadir los handlers al logger
        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)

        # Evitar la propagación de los logs al logger raíz
        logger.propagate = False

    @staticmethod
    def info(message):
        Logger.logger.info(message)
    
    @staticmethod
    def warning(message):
        Logger.logger.warning(message)
    
    @staticmethod
    def error(message):
        Logger.logger.error(message)
    
    @staticmethod
    def debug(message):
        Logger.logger.debug(message)
