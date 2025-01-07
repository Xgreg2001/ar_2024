import random
import time
import threading
import os
import networkx as nx
import matplotlib.pyplot as plt
from process import Process
from snapshot import SnapshotManager

# We'll store snapshots here whenever they occur
COLLECTED_SNAPSHOTS = []
VERBOSE = True
SNAPSHOT_FOLDER = "snapshots"


def run_random_snapshot_loop(processes, snapshot_manager, stop_event, snapshot_interval=(3, 6)):
    while not stop_event.is_set():
        time.sleep(random.uniform(*snapshot_interval))
        if stop_event.is_set():
            break
        random_starter = random.choice(list(processes.keys()))

        if VERBOSE:
            print(
                f"############ Initiated snapshot by PID={random_starter} ############")

        snapshot_manager.initiate_snapshot(random_starter)

        time.sleep(0.5)
        for proc in processes.values():
            proc.receive_from_all_in_channels()

        snap_data = {pid: processes[pid].get_snapshot_result()
                     for pid in processes}
        COLLECTED_SNAPSHOTS.append((random_starter, snap_data))


def visualize_snapshot(snapshot_title, snapshot_data):
    G = nx.DiGraph()
    for pid in snapshot_data:
        G.add_node(pid)

    edge_labels = {}
    for pid, snap in snapshot_data.items():
        in_channels_state = snap["in_channels"]
        for from_pid, msgs in in_channels_state.items():
            G.add_edge(from_pid, pid)
            if msgs:
                edge_labels[(from_pid, pid)] = ",".join(msgs)
            else:
                edge_labels[(from_pid, pid)] = ""

    node_labels = {}
    for pid, snap in snapshot_data.items():
        node_labels[pid] = f"PID={pid}\nState={snap['state']}"

    pos = nx.circular_layout(G)
    plt.figure(figsize=(7, 5))
    nx.draw(G, pos, with_labels=False, node_size=1500, node_color="skyblue")
    nx.draw_networkx_labels(G, pos, labels=node_labels, font_size=9)
    nx.draw_networkx_edge_labels(
        G, pos, edge_labels=edge_labels, font_color="red")
    plt.title(snapshot_title)
    plt.axis("off")
    # plt.show()
    plt.savefig(os.path.join(SNAPSHOT_FOLDER,
                snapshot_title.replace(" ", "_") + ".pdf"))


def main():
    if os.path.exists(SNAPSHOT_FOLDER):
        for f in os.listdir(SNAPSHOT_FOLDER):
            os.remove(os.path.join(SNAPSHOT_FOLDER, f))
    else:
        os.makedirs(SNAPSHOT_FOLDER)

    random.seed(42)
    p1 = Process(1, verbose=VERBOSE)
    p2 = Process(2, verbose=VERBOSE)
    p3 = Process(3, verbose=VERBOSE)
    p4 = Process(4, verbose=VERBOSE)
    p5 = Process(5, verbose=VERBOSE)
    processes = {p.pid: p for p in [p1, p2, p3, p4, p5]}

    p1.connect_to(p2)
    p2.connect_to(p3)
    p3.connect_to(p4)
    p4.connect_to(p5)
    p5.connect_to(p1)

    p1.connect_to(p3)
    p2.connect_to(p4)
    p3.connect_to(p5)
    p4.connect_to(p1)
    p5.connect_to(p2)

    for p in processes.values():
        p.update_state(random.randint(5, 20))

    snapshot_manager = SnapshotManager(processes)
    stop_event = threading.Event()

    process_threads = []
    for pid, proc in processes.items():
        t = threading.Thread(
            target=proc.run_process_loop,
            args=(processes, stop_event, (0.5, 1.0)),
            daemon=True
        )
        t.start()
        process_threads.append(t)

    snapshot_thread = threading.Thread(
        target=run_random_snapshot_loop,
        args=(processes, snapshot_manager, stop_event, (3, 6)),
        daemon=True
    )
    snapshot_thread.start()

    run_time = 15
    time.sleep(run_time)

    stop_event.set()

    for t in process_threads:
        t.join()
    snapshot_thread.join()

    if not COLLECTED_SNAPSHOTS:
        print("No snapshots were taken.")
        return

    for i, (init_pid, snap_data) in enumerate(COLLECTED_SNAPSHOTS, start=1):
        visualize_snapshot(
            f"Snapshot #{i} (Initiated by PID={init_pid})",
            snap_data
        )


if __name__ == "__main__":
    main()
