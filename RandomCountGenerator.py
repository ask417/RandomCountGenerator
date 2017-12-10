import random
from collections import defaultdict

def weighted_selection(values_with_weights):
    total = sum(values_with_weights.values())
    r = random.uniform(0, total)
    start = 0
    for k, v in values_with_weights.iteritems():
        if start + v >= r:
            return k
        start += v


if __name__ == "__main__":
    probs = {1:50, 2:25, 3:15, 4:5, 5:5 }
    counts = defaultdict(int)

    for i in range(100000):
        counts[weighted_selection(probs)] += 1

    print counts
