import hashlib


def build_postgres_identifier(items: list[str], suffix: str) -> str:
    """Copied from django_pg_migration_tools"""

    MAX_POSTGRES_IDENTIFIER_LEN = 63
    base_name = "_".join(items + [suffix])
    if len(base_name) <= MAX_POSTGRES_IDENTIFIER_LEN:
        return base_name

    hash_len = 8
    hash_obj = hashlib.md5(base_name.encode())
    hash_val = hash_obj.hexdigest()[:hash_len]

    chop_threshold = (
        MAX_POSTGRES_IDENTIFIER_LEN
        # Includes two "_" chars.
        - (len(suffix) + hash_len + 2)
    )
    chopped_name = base_name[:chop_threshold]
    return f"{chopped_name}_{hash_val}_{suffix}"
