class CostEstimator:
    def __init__(self, metadata):
        self.metadata = metadata

    def estimate_benefit(self, col, usage_stats):
        # A simple heuristic-based cost estimator
        # High frequency filtering and joining get higher scores
        score = 0
        score += usage_stats['where'].get(col, 0) * 10
        score += usage_stats['join'].get(col, 0) * 15
        score += usage_stats['groupby'].get(col, 0) * 8
        score += usage_stats['orderby'].get(col, 0) * 5
        return score
