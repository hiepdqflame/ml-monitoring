"""One canonical feature contract for training, serving and drift demos."""
import hashlib

import numpy as np
from sklearn.datasets import load_wine
from sklearn.model_selection import train_test_split

FEATURE_NAMES = list(load_wine().feature_names)


def load_dataset():
    return load_wine(as_frame=True).frame.copy()


def validate_dataset(frame):
    if list(frame.columns) != FEATURE_NAMES + ['target']:
        raise ValueError('Expected the 13 ordered sklearn Wine features and target')
    if len(frame) < 100 or frame.duplicated().any():
        raise ValueError('Dataset is too small or contains duplicate rows')
    if not np.isfinite(frame.to_numpy(dtype=float)).all():
        raise ValueError('Dataset contains missing or nonfinite values')
    if set(frame.target) != {0, 1, 2} or frame.target.value_counts().min() < 5:
        raise ValueError('Expected three Wine classes with at least five rows each')
    return {'rows': len(frame), 'features': len(FEATURE_NAMES),
            'sha256': hashlib.sha256(frame.to_csv(index=False).encode()).hexdigest()}


def split_dataset(frame):
    validate_dataset(frame)
    return train_test_split(frame, test_size=.2, stratify=frame.target, random_state=42)
