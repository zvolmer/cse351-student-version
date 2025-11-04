import threading
import multiprocessing as mp
import time
import cse351

def do_work(thread_id, lock, counts, barrier, quene):
    print(f"Process {thread_id} is called: {__name__}.")
    x = 0
    for i in range(100_000_000):
        x += 1

    barrier.wait

    with lock:
        counts[thread_id] = x
        print(f"{thread_id}: Work done: x was {x}")



if __name__ == '__main__':
    print("Hello World!")
    start_time = time.time()
    lock = mp.Lock()
    barrier = mp.Barrier(num_threads)
    quene = mp.Quene()
    num_threads = 3
    counts = [0 for _ in range(num_threads)]
    ts = [mp.Process(target=do_work, args=(id, lock, counts)) for id in range(3)]
    for t in ts:
        t.start()
    print("threads started")
    for t in ts:
        t.join()
    for _ in range(num_threads):
        id, result = quene.get()
        counts[id] = result
    elapsed_time = time.time() - start_time
    print(f"finished with all the work {elapsed_time} \n counts: {counts}")
