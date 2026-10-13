"""
S3Store: an s3 bucket (aws or minio) as the claim check store.
"""

from contextlib import AsyncExitStack

from eventbase.layout import place
from eventbase.metadata import decode, encode
from eventbase.store.store import Store


class S3Store(Store):
    """
    Content is an object under prefix and metadata is its user
    metadata, one x-amz-meta entry per top level key with a json
    value. S3 lowercases those keys and caps them at 2KB per object,
    so keep metadata small and its keys lowercase.

    layout names the object under prefix. It is a format string over
    the published metadata's top level fields and key, the message
    key, which wins over a metadata field of the same name:
    "{tenant}/{date}/{key}" files by tenant and date. Publishing
    without a field the layout names is refused. Keep key in the
    layout unless each message's fields alone are unique, or objects
    overwrite each other. The claim carries the full object key, so
    consumers redeem it whatever layout the producer used.

    Credentials come from the usual aws chain unless given in options,
    which go to the aiobotocore client (aws_access_key_id, config).
    Needs eventbase[s3].
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        endpoint: str | None = None,
        region: str | None = None,
        layout: str = "{key}",
        **options,
    ) -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.layout = layout
        self.endpoint = endpoint
        self.region = region
        self.options = options
        self._stack = None
        self._client = None

    async def connect(self) -> None:
        """Open the s3 client."""
        from aiobotocore.session import get_session

        if self._client is not None:
            return
        stack = AsyncExitStack()
        self._client = await stack.enter_async_context(
            get_session().create_client(
                "s3",
                endpoint_url=self.endpoint,
                region_name=self.region,
                **self.options,
            )
        )
        self._stack = stack

    async def disconnect(self) -> None:
        """Close the s3 client."""
        stack, self._stack, self._client = self._stack, None, None
        if stack is not None:
            await stack.aclose()

    async def ping(self) -> bool:
        """True when the bucket exists and these credentials reach it."""
        if self._client is None:
            return False
        await self._client.head_bucket(Bucket=self.bucket)
        return True

    def name(self, key: str, metadata: dict) -> str:
        """The object key for a message key, laid out from its metadata."""
        path = place(self.layout, key, metadata)
        return f"{self.prefix}/{path}" if self.prefix else path

    async def put(self, key: str, content: bytes, metadata: dict) -> dict:
        """Upload content with metadata as the object's user metadata."""
        if self._client is None:
            raise RuntimeError("connect before publishing")
        name = self.name(key, metadata)
        await self._client.put_object(
            Bucket=self.bucket, Key=name, Body=content, Metadata=encode(metadata)
        )
        return {"store": "s3", "bucket": self.bucket, "key": name}

    async def get(self, claim: dict) -> tuple[bytes, dict]:
        """Download the object a claim names, from this store's bucket."""
        if self._client is None:
            raise RuntimeError("connect before polling")
        response = await self._client.get_object(Bucket=self.bucket, Key=str(claim["key"]))
        async with response["Body"] as body:
            content = await body.read()
        return content, decode(response.get("Metadata", {}).items())
