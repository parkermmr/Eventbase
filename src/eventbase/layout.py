"""
Layouts name where a store parks content, from its key and metadata.
"""


def place(layout: str, key: str, metadata: dict) -> str:
    """
    Fill layout, a format string, from the metadata's top level fields
    and key, which wins over a metadata field of the same name. A field
    the layout names but the metadata lacks is refused.
    """
    try:
        return layout.format_map({**metadata, "key": key})
    except KeyError as missing:
        raise ValueError(f"layout {layout!r} needs metadata field {missing}") from None
