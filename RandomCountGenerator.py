import random
import threading
from Queue import Queue
from collections import defaultdict
from datetime import datetime, date

def weighted_selection(values_with_weights):
    '''
    Randomly select variables proportional to their corresponding weights

    :param dict values_with_weights: Labels with their relative weight
    :return: the randomly selected value according to the given distribution
    :rtype: int
    '''
    total = sum(values_with_weights.values())
    r = random.uniform(0, total)
    start = 0
    for k, v in values_with_weights.iteritems():
        if start + v >= r:
            return k
        start += v

class RandomCountGenerator:
    """
    Object for generating weighted random selections and tracking a rolling
    element frequency
    ...
    Attributes
    ----------
    values_with_weights : dict
        Labels with their relative weights
    """
    def __init__(self, values_with_weights):
        self.last_100_elements = Queue(maxsize=100)
        self.output_queue = Queue()
        self.values_with_weights = values_with_weights
        self.rolling_counts = {k:0 for k in self.values_with_weights.keys()}
        self.log_file = open("random_selections.log", "wb")
        writer = threading.Thread(target=self.write_multi_threaded)
        writer.start()


    def weighted_selection(self):
        '''
        Randomly select variables proportional to their corresponding weights

        :param dict values_with_weights: Labels with their relative weight
        :return: the randomly selected value according to the given distribution
        :rtype: int
        '''
        total = sum(self.values_with_weights.values())
        r = random.uniform(0, total)
        start = 0
        for k, v in self.values_with_weights.iteritems():
            if start + v >= r:
                self.add_element(k)
                return k
            start += v

    def write_single_threaded(self):
        now = str(datetime.now())
        self.log_file.write("{0} {1}\n".format(self.last_100_elements.queue[-1], now))

    #Should be spinning and listening to the Queue
    def write_multi_threaded(self):
        while True:
            item = self.output_queue.get(block=True)
            self.log_file.write(item)


    #Basically, we'll need to add a lock that
    def add_element(self, element):
        now = str(datetime.now())
        pair = "{0} {1}\n".format(now, element)
        self.output_queue.put(pair)
        #Probably need to add a lock here so we don't get any race conditions
        if self.last_100_elements.qsize() < 100:
            self.last_100_elements.put(element)
            self.update_counts(element)
        else:
            removed = self.last_100_elements.get()
            self.last_100_elements.put(element)
            self.update_counts(element, removed)


    def update_counts(self, added, removed = None):
        if removed is not None:
            self.rolling_counts[removed] -= 1
        self.rolling_counts[added] += 1

    def get_frequencies(self):
        return {k:v/100.0 for k,v in self.rolling_counts.iteritems()}


if __name__ == "__main__":
    probs = {1:50, 2:25, 3:15, 4:5, 5:5 }
    counts = defaultdict(int)

    for i in range(100000):
        counts[weighted_selection(probs)] += 1

    print counts

    rcg = RandomCountGenerator(probs)

    for i in range(100000):
        rcg.weighted_selection()
        if i%10000 == 0:
            print rcg.get_frequencies()
            rcg.write_single_threaded()

