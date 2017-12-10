import random
import time
import threading
from Queue import Queue
from collections import defaultdict
from datetime import datetime


#First iteration as function
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
    num_random_selectors: int
        Number of random number generators we'll have running concurrently
    log_filename: str
        Log filename
    """
    def __init__(self, values_with_weights, num_random_selectors, log_filename):
        self.last_100_elements = Queue(maxsize=100)
        self.rolling_counts = {k:0 for k in values_with_weights.keys()}
        self._total = sum(values_with_weights.values())
        self._output_queue = Queue()
        self._values_with_weights = values_with_weights
        self._log_file = open(log_filename, "wb")
        self._lock = threading.Lock()
        self._active_threads = dict()
        self._active_threads["writer"] = threading.Thread(target=self.write_multi_threaded)
        self._active_threads["writer"].start()

        for i in range(num_random_selectors):
            self._active_threads[i] = threading.Thread(target=self.weighted_selection_continuous)
            self._active_threads[i].start()

    def threads_completed(self):
        return all([not thread.isAlive() for thread in self._active_threads.values()])

    def weighted_selection_continuous(self):
        '''
        Continuously calls weighted_selection, runs as thread

        :return: None
        '''
        t_end = time.time() + 5
        while time.time() < t_end:
            self.weighted_selection()

    def weighted_selection(self):
        '''
        Randomly select variables proportional to their corresponding weights

        :param dict values_with_weights: Labels with their relative weight
        :return: The randomly selected value according to the given distribution
        :rtype: int
        '''
        r = random.uniform(0, self._total)
        start = 0
        for k, v in self._values_with_weights.iteritems():
            if start + v >= r:
                self.add_element(k)
                return k
            start += v

    def write_single_threaded(self):
        '''
        Writes most recently selected number to file along with current time

        :return: None
        '''
        now = str(datetime.now())
        self._log_file.write("{0} {1}\n".format(self.last_100_elements.queue[-1], now))

    def write_multi_threaded(self):
        '''
        Continuously pulls last added number from the queue, writes to file with
        current time.  Runs as thread.

        :return: None
        '''
        t_end = time.time() + 5
        while time.time() < t_end:
            item = self._output_queue.get(block=True)
            self._log_file.write(item)


    def add_element(self, element):
        '''
        Thread safe method to add element to both rolling queue and
        output queue and also to update rolling counts

        :param int element: Randomly selected element from values_with_weights
        :return: None
        '''
        with self._lock:
            now = str(datetime.now())
            pair = "{0} {1}\n".format(now, element)
            self._output_queue.put(pair)
            if self.last_100_elements.qsize() < 100:
                self.last_100_elements.put(element)
                self.update_counts(element)
            else:
                removed = self.last_100_elements.get()
                self.last_100_elements.put(element)
                self.update_counts(element, removed)


    def update_counts(self, added, removed = None):
        '''
        Update rolling counts for each element from values_with_weights

        :param int added: Newly added element
        :param int removed: Last element dequeued or None
        :return: None
        '''
        if removed is not None:
            self.rolling_counts[removed] -= 1
        self.rolling_counts[added] += 1

    def get_frequencies(self):
        '''
        Return dictionary of frequencies for rolling element counts

        :return: Map of frequency percentages
        :rtype: dict
        '''
        return {k: "{0}%".format(100 * float(v) / self._total) for k, v in self.rolling_counts.iteritems()}



def check_output(filename):
    previous = datetime.min
    no_gaps = True
    with open(filename, "rb") as f:
        lines = f.readlines()
        for line in lines:
            try:
                items = line.split()
                dt = items[0] + " " + items[1]
                dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
                if dt < previous:
                    print "Sequence gap: {0} | {1}".format(str(previous), str(dt))
                    no_gaps = False
                previous = dt
            except Exception as e:
                print e
    output = "No time gaps observed" if no_gaps else "Log contains time gaps"
    print output



if __name__ == "__main__":
    probs = {1:50, 2:25, 3:15, 4:5, 5:5}
    log_filename = "random_selections.log"
    counts = defaultdict(int)
    num_random_selectors = 5

    for i in range(100000):
        counts[weighted_selection(probs)] += 1
    print counts

    rcg = RandomCountGenerator(probs, num_random_selectors, log_filename)
    while not rcg.threads_completed():
        time.sleep(1)
    time.sleep(1)
    check_output(log_filename)
    
    #Testing from single threaded version
    # for i in range(100000):
    #     rcg.weighted_selection()
    #     if i%10000 == 0:
    #         print rcg.get_frequencies()
    #         rcg.write_single_threaded()

