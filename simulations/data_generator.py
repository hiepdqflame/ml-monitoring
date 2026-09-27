"""Generate samples from the same 13-feature Wine contract used by training."""
import numpy as np
import pandas as pd
from sklearn.datasets import load_wine
import yaml


class WineDataGenerator:
    def __init__(self, config_path="config.yaml"):
        with open(config_path) as source:
            self.config = yaml.safe_load(source)
        self.wine = load_wine()
        self.feature_names = list(self.wine.feature_names)
        if list(self.config['features']) != self.feature_names:
            raise ValueError("Simulation features must match sklearn Wine training order")
        self.rng = np.random.default_rng(42)

    def get_feature_names(self):
        return self.feature_names.copy()

    def generate_normal_sample(self):
        row = self.wine.data[self.rng.integers(len(self.wine.data))].copy()
        return dict(zip(self.feature_names, map(float, row)))

    def generate_drifted_sample(self, drift_multiplier=1.5, affected_features=None, noise_level=.2):
        sample = self.generate_normal_sample()
        affected = self.feature_names if affected_features is None else affected_features
        for name in affected:
            index = self.feature_names.index(name)
            scale = self.wine.data[:, index].std()
            sample[name] += float((drift_multiplier - 1) * 4 * scale + self.rng.normal(0, scale * noise_level))
        return sample

    def generate_batch(self, n_samples=100, scenario="normal"):
        options = self.config['scenarios'][scenario]
        batch = []
        for _ in range(n_samples):
            if scenario == 'normal':
                batch.append(self.generate_normal_sample())
            else:
                features = self.rng.choice(self.feature_names, size=min(options.get('affected_features', 4), 13), replace=False)
                batch.append(self.generate_drifted_sample(options.get('drift_multiplier', 1.5), features, options.get('noise_level', .2)))
        return batch

    def generate_dataframe(self, n_samples=100, scenario='normal'):
        return pd.DataFrame(self.generate_batch(n_samples, scenario))
