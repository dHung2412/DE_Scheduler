import os


def load_sql_from_file(file_path: str, **kwargs) -> str:
    if not os.path.exists(file_path):     
        raise FileNotFoundError(f"Không tìm thấy file SQL tại: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    return sql_content.format(**kwargs)
