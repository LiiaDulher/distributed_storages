import hazelcast
import sys
import time
import multiprocessing


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
    print("Starting", client_number)
    for i in range(1000):
        value = distributed_map.get(key)
        time.sleep(0.01)
        value += 1
        distributed_map.put(key, value)
    print("Finished", client_number, "!  Result =", distributed_map.get(key))


def pessimistic_locking(client_number):
    client = hazelcast.HazelcastClient()
    distributed_map = client.get_map("map2").blocking()
    key = "1"
    distributed_map.put(key, 0)
    print("Starting", client_number)
    for i in range(1000):
        distributed_map.lock(key)
        try:
            value = distributed_map.get(key)
            time.sleep(0.01)
            value += 1
            distributed_map.put(key, value)
        finally:
            distributed_map.unlock(key)
    print("Finished", client_number, "!  Result =", distributed_map.get(key))


def optimistic_locking(client_number):
    client = hazelcast.HazelcastClient()
    distributed_map = client.get_map("map3").blocking()
    key = "1"
    distributed_map.put(key, 0)
    print("Starting", client_number)
    for i in range(1000):
        while True:
            value = distributed_map.get(key)
            time.sleep(0.01)
            new_value = value + 1
            if distributed_map.replace(key, value, new_value):
                break
    print("Finished", client_number, "! Result =", distributed_map.get(key))


def test_map_with_lock():
    print("Test with no locking: ")
    no_locking(1)
    # print("Test with pessimistic locking: ")
    # pessimistic_locking(1)
    # print("Test with optimistic locking: ")
    # optimistic_locking(1)


def bounded_queue_client(client_type):
    if client_type == "r":
        pass
    elif client_type == "w":
        pass
    else:
        return


def test_bounded_queue():
    pass


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
        test_bounded_queue()
    else:
        print("No task with such number.")


if __name__ == "__main__":
    main()
