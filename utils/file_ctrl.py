
import os

def save_file(file_path: str, title: str):
    """파일 저장"""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(title)
    print(f"저장 완료: {file_path}")