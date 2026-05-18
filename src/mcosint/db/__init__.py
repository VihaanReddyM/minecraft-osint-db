"""Database access layer (PostgreSQL).

This package isolates:
- connection pooling (`connection.py`)
- schema + indexes (`schema.py`)
- write operations / upserts (`operations.py`)

The crawler threads should never build SQL inline; call functions here instead.
"""
