from .health import update as health_topic_update
from .logger import logger


def health_monthly():
    """Update monthly charts in health page"""

    # Run update scripts
    health_topic_update.update_monthly()
    logger.info("Updated monthly charts in health page")


if __name__ == "__main__":
    health_monthly()
