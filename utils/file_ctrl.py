
import os
from pathlib import Path

_BASE_DIR = Path("./data").resolve()


def save_file(file_path: str, title: str):
    """파일 저장"""
    target = Path(file_path).resolve()
    if not str(target).startswith(str(_BASE_DIR)):
        raise ValueError(f"허용되지 않은 경로입니다: {file_path}")
    os.makedirs(target.parent, exist_ok=True)
    with open(target, "w", encoding="utf-8") as f:
        f.write(title)
    print(f"저장 완료: {target}")