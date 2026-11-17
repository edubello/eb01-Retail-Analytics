from pathlib import Path
import yaml

def load_settings(config_path: str | Path | None = None) -> dict:
    """Carrega o arquivo settings.yaml resolvendo automaticamente a raiz do projeto."""
    if config_path is None:
        current_path = Path.cwd()
        project_root = current_path.parent if current_path.name in ("src", "notebooks") else current_path
        config_path = project_root / "config" / "settings.yaml"
    
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)