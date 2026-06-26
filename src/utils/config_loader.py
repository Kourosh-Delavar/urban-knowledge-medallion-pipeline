import os
import yaml
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_pipeline_config() -> dict:

    logger.info("Load pipeline configurations...")

    current_file = Path(__file__).resolve()
    project_root = current_file.parents[2]
    config_path = project_root / "config" / "pipeline_config.yaml"

    logger.info(f"Resolved configuration file path: {config_path}")

    if not config_path.exists():
        logger.error(f"Configuration file not found: {config_path}") 
        raise FileExistsError(f"Configuration file missing at expected path: {config_path}")
    
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)

        logger.info("Pipeline configurations successfully loaded.")
        return config
    
    except yaml.YAMLError as yaml_error:
        logger.error(f"Failed to parse YAML configurations from the file: {str(yaml_error)}")
        raise
    except Exception as general_error:
        logger.error(f"Unexpected file error while reading configurations: {str(general_error)}")
        raise

def get_env_variable(key: str, default: str = None) -> str:

    value = os.getenv(key, default)

    if value is None:
        logger.error(f"Environment variable failed for mandatory key: {key}")
        raise ValueError(f"Environment variable '{key}' is missing.")
    
    return value