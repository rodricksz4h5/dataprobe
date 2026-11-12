"""dataprobe Hermes plugin — register three read-only data-file tools."""
from . import dataprobe as _dp


def register(ctx):
    ctx.register_tool(
        name="data_profile",
        toolset="dataprobe",
        schema=_dp.DATA_PROFILE,
        handler=_dp.data_profile,
    )
    ctx.register_tool(
        name="data_validate",
        toolset="dataprobe",
        schema=_dp.DATA_VALIDATE,
        handler=_dp.data_validate,
    )
    ctx.register_tool(
        name="data_head",
        toolset="dataprobe",
        schema=_dp.DATA_HEAD,
        handler=_dp.data_head,
    )
