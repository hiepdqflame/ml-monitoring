"""Promotion decisions and failure-preserving DAG summaries."""
import math


def quality_gate(candidate, champion=None, min_accuracy=.9, min_f1=.9):
    reasons = []
    for key, minimum in [('accuracy', min_accuracy), ('f1_macro', min_f1)]:
        if not math.isfinite(minimum) or not 0 <= minimum <= 1:
            raise ValueError('Quality thresholds must be finite values from 0 to 1')
        value = candidate.get(key, float('nan'))
        if not math.isfinite(value) or not 0 <= value <= 1 or value < minimum:
            reasons.append(key + ' below threshold or invalid')
        if champion is not None:
            previous = champion.get(key, float('nan'))
            if not math.isfinite(previous) or not 0 <= previous <= 1 or value < previous:
                reasons.append(key + ' regresses against Production or comparison is invalid')
    return {'passed': not reasons, 'reasons': reasons}


def pipeline_status(states, promoted=False):
    if any(state not in ('success', 'skipped') for state in states):
        return 'FAILED'
    return 'PROMOTED' if promoted else 'REJECTED'
