import logging
import logging.config

# Set Log File
logging.config.fileConfig("logging.conf")

# Create Logger
logger = logging.getLogger("Coast")

# Application Code
logger.debug("debug message")
logger.info("info message")
logger.warn("warn message")
logger.error("error message")
logger.critical("critical message")

