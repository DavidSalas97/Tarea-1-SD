import hashlib
import os
import ijson
import json
import time
import random
import numpy as np
import pymemcache.client

# Get the path of the current file with os
path = os.path.dirname(os.path.abspath(__file__))

def read_json_file(filename, client, num_items=0):
    with open(filename, 'rb') as f:
        parser = ijson.parse(f)
        count = 0
        for prefix, event, value in parser:
            if prefix.endswith('.id') and event == 'number':
                data = {'id': value}
                for prefix, event, value in parser:
                    if prefix.endswith('.make') and event == 'string':
                        data['make'] = value
                    if prefix.endswith('.model') and event == 'string':
                        data['model'] = value
                    if prefix.endswith('.year') and event == 'number':
                        data['year'] = value
                        break
                client.set(str(data['id']), json.dumps(data))
                print(json.dumps(data, indent=4))
                count += 1
                if num_items != 0 and count >= num_items:
                    break
        if num_items == 0 or count < num_items:
            print(f"Printed {count} items.")
        else:
            print(f"Printed {num_items} items.")

def search_by_id(filename, id, client):
    start_time = time.perf_counter()
    data = client.get(str(id))
    if data is not None:
        print(data.decode('utf-8'))
        elapsed_time = time.perf_counter() - start_time
        print(f"Time taken: {elapsed_time:.6f} seconds")
        print("Data retrieved from cache.")
        return
    with open(filename, 'rb') as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix.endswith('.id') and event == 'number':
                if value == id:
                    data = {'id': value}
                    for prefix, event, value in parser:
                        if prefix.endswith('.make') and event == 'string':
                            data['make'] = value
                        if prefix.endswith('.model') and event == 'string':
                            data['model'] = value
                        if prefix.endswith('.year') and event == 'number':
                            data['year'] = value
                            break
                    client.set(str(data['id']), json.dumps(data))
                    print(f"Added item with ID {id} to cache.")
                    print(json.dumps(data, indent=4))
                    elapsed_time = time.perf_counter() - start_time
                    print(f"Time taken: {elapsed_time:.6f} seconds")
                    return
        print(f"No item with ID {id} found.")
        elapsed_time = time.perf_counter() - start_time
        print(f"Time taken: {elapsed_time:.6f} seconds")

def search_by_ids(filename, id, client):
    data = client.get(str(id))
    if data is not None:
        print(data.decode('utf-8'))
        print("Data retrieved from cache.")
        return
    
    with open(filename, 'rb') as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix.endswith('.id') and event == 'number':
                if value == id:
                    data = {'id': value}
                    for prefix, event, value in parser:
                        if prefix.endswith('.make') and event == 'string':
                            data['make'] = value
                        if prefix.endswith('.model') and event == 'string':
                            data['model'] = value
                        if prefix.endswith('.year') and event == 'number':
                            data['year'] = value
                            break
                    client.set(str(data['id']), json.dumps(data))
                    print(f"Added item with ID {id} to cache.")
                    print(json.dumps(data, indent=4))
                    return
        print(f"No item with ID {id} found.")

def search_by_ids_txt(filename, ids_file, client):

    # start timer
    start_time = time.perf_counter()
    print("Searching for IDs...")
    with open(ids_file, 'r') as f:
        ids = [int(line.strip()) for line in f]

    for id in ids:
        search_by_ids(filename, id, client)
    #end timer
    elapsed_time = time.perf_counter() - start_time
    print(f"Time taken: {elapsed_time:.6f} seconds")

def generate_ids(mean, std_dev, num_ids):
    ids = np.random.normal(mean, std_dev, num_ids)
    ids = [int(id) for id in ids if id >= 0]  # Convert to integers and remove negative values
    path = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(path, 'ids.txt')
    with open(file_path, 'w') as f:  # This will clear the file before writing to it
        for id in ids:
            f.write(str(id) + '\n')

#
def hash_key(key):
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16) % 2
# load balancer using two clients, docker and local
def load_balancer(filename, id, local_client, docker_client):
    key = str(id)
    client = [local_client, docker_client][hash_key(key)]
    data = client.get(key)
    if data is not None:
        print(data.decode('utf-8'))
        print("Data retrieved from cache.")
        return
    
    with open(filename, 'rb') as f:
        parser = ijson.parse(f)
        for prefix, event, value in parser:
            if prefix.endswith('.id') and event == 'number':
                if value == id:
                    data = {'id': value}
                    for prefix, event, value in parser:
                        if prefix.endswith('.make') and event == 'string':
                            data['make'] = value
                        if prefix.endswith('.model') and event == 'string':
                            data['model'] = value
                        if prefix.endswith('.year') and event == 'number':
                            data['year'] = value
                            break
                    client.set(key, json.dumps(data))
                    print(f"Added item with ID {id} to cache.")
                    print(json.dumps(data, indent=4))
                    return
        print(f"No item with ID {id} found.")

def clear_cache(client):
    client.flush_all()
    print("Cache cleared.")

def distrubucion_normal(num_queries, num_ids):
    mean = num_ids / 2
    std_dev = num_ids / 6
    ids = []
    for i in range(num_queries):
        while True:
            id = int(random.normalvariate(mean, std_dev))
            if id >= 0 and id < num_ids:
                ids.append(id)
                break
    return ids

def main():
    filename = path + '/cars.json'
    client = pymemcache.client.base.Client(('localhost', 11211))
    docker_client = pymemcache.client.base.Client(('friendly_swirles', 11211))
    
    # print that the client and docker client are connected
    print(client)
    print(docker_client)
    
    while True:
        print("1. Read JSON file")
        print("2. Search for a single ID")
        print("3. Clear cache")
        print("4. Consulta con distrubucion normal")
        print("5. Haz lista de ids")
        print("6. Search for multiple IDs from txt file")
        print("7. Load balancer search")
        choice = input("Enter your choice: ")

        if choice == '1':
            id = int(input("Enter the number of items to read, use 0 to print all json: "))
            read_json_file(filename, client, id)
            print()
        elif choice == '2':
            id = int(input("Enter the ID to search for: "))
            search_by_id(filename, id, client)
            print()
        elif choice == '3':
            clear_cache(client)
            print()
        elif choice == '4':
            num_queries = int(input("Enter the number of queries to generate: "))
            num_ids = 10000000  # Mientras mas grande el numero, mas se demora, ya que es busqueda secuencial
            ids = distrubucion_normal(num_queries, num_ids)

            start_time = time.perf_counter()
            for id in ids:
                search_by_ids(filename, id, client)
            elapsed_time1 = time.perf_counter() - start_time

            start_time = time.perf_counter()
            for id in ids:
                search_by_ids(filename, id, client)
            elapsed_time2 = time.perf_counter() - start_time

            print(f"Time taken for normal search: {elapsed_time1:.6f} seconds")
            print(f"Time taken for cache search: {elapsed_time2:.6f} seconds")
            print()
        elif choice == '5':
            elapsed_time = time.perf_counter() - start_time
            mean = int(input("Enter the mean of the IDs to generate: "))
            std_dev = int(input("Enter the standard deviation of the IDs to generate: "))
            num_ids = int(input("Enter the number of IDs to generate: "))
            generate_ids(mean, std_dev, num_ids)
            print(f"Time taken: {elapsed_time:.6f} seconds")
        elif choice == '6':
            # the id txt file its in the same folder as the code, and that path its called path, also the file its called ids.txt
            ids_file = path + '/ids.txt'
            elapsed_time = time.perf_counter() - start_time
            search_by_ids_txt(filename, ids_file, client)
            print(f"Time taken: {elapsed_time:.6f} seconds")
            print()

        elif choice == '7':
            elapsed_time = time.perf_counter() - start_time
            id = int(input("Enter the ID to search for: "))
            load_balancer(filename, id, client, docker_client)
            print(f"Time taken: {elapsed_time:.6f} seconds")
            print()
        else:
            print("Invalid choice. Please try again.")
            
if __name__ == '__main__':
    main()