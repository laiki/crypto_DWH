#!/usr/bin/env python
"""
Reserved entrypoint for future gradient-based fine-tuning of AI forecasting models.

This path is intentionally kept separate from `train_ai_models.py`.
`train_ai_models.py` currently evaluates pretrained AI backends such as
Chronos2 in zero-shot mode and registers artifacts/metrics without updating
model weights.

Planned scope for this script:
1. Load one or more training datasets with an explicit train/validation split.
2. Fine-tune selected AI backends with optimizer-based weight updates.
3. Persist fine-tuned checkpoints separately from zero-shot artifacts.
4. Register fine-tuned model metadata in the same forecast registry with a
   distinct execution mode.
"""

from __future__ import annotations

import argparse


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Reserved entrypoint for future gradient-based AI model fine-tuning. "
            "Not implemented yet."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--backend",
        default="chronos2",
        help="Target AI backend planned for future fine-tuning support.",
    )
    parser.add_argument(
        "--training-db",
        nargs="+",
        default=None,
        help="Planned training dataset inputs for future fine-tuning runs.",
    )
    parser.add_argument(
        "--validation-db",
        nargs="+",
        default=None,
        help="Planned validation dataset inputs for future fine-tuning runs.",
    )
    return parser.parse_args()


def main() -> None:
    _ = parse_args()
    raise SystemExit(
        "fine_tune_ai_models.py is intentionally reserved for future gradient-based fine-tuning. "
        "The current Chronos2 path is implemented in train_ai_models.py as pretrained zero-shot evaluation "
        "and registration only."
    )


if __name__ == "__main__":
    main()
