import hazelcast
import sys
import time
import threading


def test_information_distribution_on_nodes():
    hz = hazelcast.HazelcastClient()
    test_map = hz.get_map("test-distributed-map").blocking()
    for i in range(1000):
        test_map.put(i, str(i))
    hz.shutdown()


def no_locking(client_number):
    client = hazelcast.HazelcastClient()
    distributed_map = client.get_map("map1").blocking()
    key = "1"
    distributed_map.put(key, 0)
    lock = threading.Lock()
    lock.acquire()
    try:
        print("Starting client", client_number)
    finally:
        lock.release()
    for i in range(1000):
        value = distributed_map.get(key)
        time.sleep(0.01)
        value += 1
        distributed_map.put(key, value)
    lock.acquire()
    try:
        print("Finished client", client_number, "!  Result =", distributed_map.get(key))
    finally:
        lock.release()
    client.shutdown()


def pessimistic_locking(client_number):
    client = hazelcast.HazelcastClient()
    distributed_map = client.get_map("map2").blocking()
    key = "1"
    distributed_map.put(key, 0)
    lock = threading.Lock()
    lock.acquire()
    try:
        print("Starting client", client_number)
    finally:
        lock.release()
    for i in range(1000):
        distributed_map.lock(key)
        try:
            value = distributed_map.get(key)
            time.sleep(0.01)
            value += 1
            distributed_map.put(key, value)
        finally:
            distributed_map.unlock(key)
    lock.acquire()
    try:
        print("Finished client", client_number, "!  Result =", distributed_map.get(key))
    finally:
        lock.release()
    client.shutdown()


def optimistic_locking(client_number):
    client = hazelcast.HazelcastClient()
    distributed_map = client.get_map("map3").blocking()
    key = "1"
    distributed_map.put(key, 0)
    lock = threading.Lock()
    lock.acquire()
    try:
        print("Starting client", client_number)
    finally:
        lock.release()
    for i in range(1000):
        while True:
            value = distributed_map.get(key)
            time.sleep(0.01)
            new_value = value + 1
            if distributed_map.replace_if_same(key, value, new_value):
                break
    lock = threading.Lock()
    lock.acquire()
    try:
        print("Finished client ", client_number, "! Result =", distributed_map.get(key))
    finally:
        lock.release()
    client.shutdown()


def test_map_with_lock():
    print("Test with no locking: ")
    client1 = threading.Thread(target=no_locking, args=(1,))
    client2 = threading.Thread(target=no_locking, args=(2,))
    client3 = threading.Thread(target=no_locking, args=(3,))
    client1.start()
    client2.start()
    client3.start()
    client1.join()
    client2.join()
    client3.join()
    print("------------")
    print("Test with pessimistic locking: ")
    client1 = threading.Thread(target=pessimistic_locking, args=(1,))
    client2 = threading.Thread(target=pessimistic_locking, args=(2,))
    client3 = threading.Thread(target=pessimistic_locking, args=(3,))
    client1.start()
    client2.start()
    client3.start()
    client1.join()
    client2.join()
    client3.join()
    print("------------")
    print("Test with optimistic locking: ")
    client1 = threading.Thread(target=optimistic_locking, args=(1,))
    client2 = threading.Thread(target=optimistic_locking, args=(2,))
    client3 = threading.Thread(target=optimistic_locking, args=(3,))
    client1.start()
    client2.start()
    client3.start()
    client1.join()
    client2.join()
    client3.join()


def bounded_queue_client(client_type, client_number):
    lock = threading.Lock()
    if client_type == "r":
        if client_number == -1:
            time.sleep(10)
            lock.acquire()
            try:
                print("Starting reading client")
            finally:
                lock.release()
            client = hazelcast.HazelcastClient()
            queue = client.get_queue("bounded_queue").blocking()
            lock.acquire()
            try:
                print("Reading element from queue")
            finally:
                lock.release()
            queue.take()
            lock.acquire()
            try:
                print("Finished reading client")
            finally:
                lock.release()
            client.shutdown()
            return
        lock.acquire()
        try:
            print("Starting reading client", client_number)
        finally:
            lock.release()
        client = hazelcast.HazelcastClient()
        queue = client.get_queue("bounded_queue").blocking()
        numbers = []
        while True:
            n = queue.take()
            if n == -1:
                break
            numbers.append(n)
        queue.put(-1)
        lock.acquire()
        try:
            print("Client ", client_number, " read ", numbers)
        finally:
            lock.release()
        lock.acquire()
        try:
            print("Client ", client_number, " read ", len(numbers),  " elements from queue")
        finally:
            lock.release()
        lock.acquire()
        try:
            print("Finished reading client", client_number)
        finally:
            lock.release()
        client.shutdown()
    elif client_type == "w":
        lock.acquire()
        try:
            print("Starting writing client")
        finally:
            lock.release()
        client = hazelcast.HazelcastClient()
        queue = client.get_queue("bounded_queue").blocking()
        lock.acquire()
        try:
            print("Putting values into queue.")
        finally:
            lock.release()
        for i in range(100):
            queue.put(i)
        queue.put(-1)
        if client_number == 2:
            lock.acquire()
            try:
                print("Trying to put into full queue:")
            finally:
                lock.release()
            queue.put(-2)
            lock.acquire()
            try:
                print("Extra put done.")
            finally:
                lock.release()
        lock.acquire()
        try:
            print("Finished writing client")
        finally:
            lock.release()
        client.shutdown()
    else:
        return


def test_bounded_queue(test_type):
    if test_type == 1:
        client1 = threading.Thread(target=bounded_queue_client, args=("w", 2,))
        client2 = threading.Thread(target=bounded_queue_client, args=("r", -1,))
        client1.start()
        client2.start()
        client1.join()
        client2.join()
        return
    client1 = threading.Thread(target=bounded_queue_client, args=("r", 1,))
    client2 = threading.Thread(target=bounded_queue_client, args=("r", 2,))
    client3 = threading.Thread(target=bounded_queue_client, args=("w", 1,))
    print("Start writing items into bounded queue.")
    client3.start()
    client3.join()
    print("Start reading items from queue.")
    client1.start()
    client2.start()
    client1.join()
    client2.join()


def main():
    if len(sys.argv) != 2:
        print("Wrong arguments number. Please choose task to run.")
        return
    task = sys.argv[1]
    if task == "1":
        test_information_distribution_on_nodes()
    elif task == "2":
        test_map_with_lock()
    elif task == "3":
        test_bounded_queue(1)
    elif task == "4":
        test_bounded_queue(2)
    else:
        print("No task with such number.")


if __name__ == "__main__":
    main()
