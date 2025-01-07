import multiprocessing
import os
import sys
import collections


def worker_map(pipe_conn):
    print(f"MAP worker {os.getpid()} started.")
    word_counts = collections.Counter()

    while True:
        data = pipe_conn.recv()
        if data == 'DONE':
            print(f"MAP worker {os.getpid()} done.")
            break
        # data is a chunk of text
        for word in data.split():
            word_counts[word] += 1

    pipe_conn.send(dict(word_counts))
    pipe_conn.send('DONE')
    pipe_conn.close()
    print(f"MAP worker {os.getpid()} exiting.")


def worker_reduce(pipe_conn):
    print(f"REDUCE worker {os.getpid()} started.")
    word_counts = collections.Counter()
    while True:
        data = pipe_conn.recv()
        if data == 'DONE':
            print(f"REDUCE worker {os.getpid()} done.")
            break
        # data is a list of (word, counts)
        for (word, count) in data:
            word_counts[word] += count

    pipe_conn.send(dict(word_counts))
    pipe_conn.send('DONE')
    pipe_conn.close()
    print(f"REDUCE worker {os.getpid()} exiting.")


def main():
    # read input files from args
    if len(sys.argv) < 2:
        print("Usage: python mapreduce_wordcount.py <input_file1> <input_file2> ...")
        sys.exit(1)

    input_files = sys.argv[1:]

    num_workers = multiprocessing.cpu_count()
    print(f"Using {num_workers} worker processes.")

    print("Creating MAP workers...")
    # Create pipes for communication with MAP workers
    map_pipes = []
    map_workers = []
    for _ in range(num_workers):
        parent_conn, child_conn = multiprocessing.Pipe(duplex=True)
        p = multiprocessing.Process(target=worker_map, args=(child_conn,))
        p.start()
        map_pipes.append(parent_conn)
        map_workers.append(p)

    print("Reading input Files...")
    # Read input files and distribute chunks to MAP workers
    file_chunks = []
    for filename in input_files:
        with open(filename, 'r') as f:
            file_chunks.extend(f.readlines())

    print("Distributing chunks to MAP workers...")
    # Distribute chunks to workers
    for i, chunk in enumerate(file_chunks):
        map_pipes[i % num_workers].send(chunk)

    print("Distribution complete.")
    # Send 'DONE' signal to MAP workers
    for pipe in map_pipes:
        pipe.send('DONE')

    print("Collecting MAP results...")
    # Collect MAP results
    intermediate_results = []
    for pipe in map_pipes:
        while True:
            try:
                result = pipe.recv()
                if result == 'DONE':
                    break
                intermediate_results.append(result)
            except EOFError:
                break

    print("waiting for MAP workers to finish...")
    # Wait for MAP workers to finish
    for p in map_workers:
        p.join()

    print("MAP phase complete.")
    print("Starting REDUCE phase...")
    keys = set()
    for partial_result in intermediate_results:
        for word, count in partial_result.items():
            keys.add(word)

    print("Preparing data for REDUCE workers...")
    # Prepare data for REDUCE workers
    num_reduce_workers = num_workers

    print("Creating REDUCE workers...")
    # Create pipes for communication with REDUCE workers
    reduce_pipes = []
    reduce_workers = []
    for _ in range(num_reduce_workers):
        parent_conn, child_conn = multiprocessing.Pipe(duplex=True)
        p = multiprocessing.Process(target=worker_reduce, args=(child_conn,))
        p.start()
        reduce_pipes.append(parent_conn)
        reduce_workers.append(p)

    print("Distributing data to REDUCE workers...")
    # Distribute words to REDUCE workers
    for i, word in enumerate(keys):
        data = []
        for partial_result in intermediate_results:
            if word in partial_result.keys():
                data.append((word, partial_result[word]))
        reduce_pipes[i % num_reduce_workers].send(data)

    print("Distribution complete.")
    # Send 'DONE' signal to REDUCE workers
    for pipe in reduce_pipes:
        pipe.send('DONE')

    print("Collecting REDUCE results...")
    # Collect REDUCE results
    final_results = {}
    for pipe in reduce_pipes:
        while True:
            try:
                result = pipe.recv()
                if result == 'DONE':
                    break
                for word, count in result.items():
                    if word in final_results:
                        final_results[word] += count
                    else:
                        final_results[word] = count
            except EOFError:
                break

    print("Waiting for REDUCE workers to finish...")
    # Wait for REDUCE workers to finish
    for p in reduce_workers:
        p.join()

    print("REDUCE phase complete.")
    # Output final word counts to results.txt
    with open('results.txt', 'w') as f:
        for word, count in final_results.items():
            f.write(f"{word}: {count}\n")


if __name__ == "__main__":
    main()
