from __future__ import annotations

from enum import StrEnum

from emoda.services.pipeline import Pipeline, PipelineInput, PipelineResult


class ProcessingMode(StrEnum):
    DRY = "dry"
    LIVE = "live"


async def process_product(
    pipeline: Pipeline,
    input: PipelineInput,
    mode: ProcessingMode,
) -> PipelineResult:
    """Run the single business pipeline; mode controls only the final sink request."""
    return await pipeline.run(input, dry_run=mode is ProcessingMode.DRY)
