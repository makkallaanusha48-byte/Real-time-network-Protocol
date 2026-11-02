"""Lossy-network simulator (proxy) + experiment runner. Usage: python simulate.py"""
import heapq, json, random, select, socket, sys, threading, time
from dart import Sender, Receiver, summarize, PAYLEN

N, INTERVAL, DEADLINE, OWD_MS, JITTER_MS = 300, 10, 150, 40, 3


class Proxy(threading.Thread):
    """Sits between sender and receiver; drops packets with prob `loss` and adds delay, both directions."""
    def __init__(self, loss, recv_addr, seed):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.addr, self.loss, self.recv_addr = self.sock.getsockname(), loss, recv_addr
        self.rng, self.heap, self.snd, self.stop, self.c = random.Random(seed), [], None, False, 0

    def run(self):
        while not self.stop:
            if select.select([self.sock], [], [], 0.002)[0]:
                data, src = self.sock.recvfrom(4096)
                if src == self.recv_addr:
                    dst = self.snd
                else:
                    self.snd, dst = src, self.recv_addr
                if dst and self.rng.random() >= self.loss:
                    due = time.time() + (OWD_MS + self.rng.uniform(-JITTER_MS, JITTER_MS)) / 1000.0
                    self.c += 1
                    heapq.heappush(self.heap, (due, self.c, dst, data))
            while self.heap and self.heap[0][0] <= time.time():
                _, _, dst, data = heapq.heappop(self.heap)
                self.sock.sendto(data, dst)


def run_once(mode, loss, seed):
    rx = Receiver(mode)
    px = Proxy(loss, rx.addr, seed)
    px.start()
    tr = threading.Thread(target=rx.run, daemon=True)
    tr.start()
    tx = Sender(mode, px.addr, N, INTERVAL, DEADLINE)
    tx.run()
    tr.join(5)
    px.stop = True
    r = summarize(rx.got, N)
    base = N * (16 + PAYLEN)
    r.update(overhead=tx.bytes / base - 1, retx=tx.st["retx"], parity=tx.st["parity"],
             recovered=rx.recovered, bad=rx.bad_recover, late_skip=tx.st["late_skip"],
             prio_drop=tx.st["prio_drop"])
    return r


if __name__ == "__main__":
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    out = {}
    print("%-5s %-5s %8s %8s %8s %8s %8s" % ("mode", "loss", "on-time", "crit", "lat(ms)", "overhd", "late-rx"))
    for loss in (0.0, 0.05, 0.10, 0.20):
        for mode in ("udp", "retx", "dart"):
            rs = [run_once(mode, loss, 100 * r + int(loss * 100)) for r in range(reps)]
            avg = {k: sum(x[k] for x in rs) / reps for k in rs[0]}
            out["%s@%.2f" % (mode, loss)] = avg
            print("%-5s %4.0f%% %7.1f%% %7.1f%% %8.1f %7.1f%% %8.1f" % (
                mode, loss * 100, avg["on_time"] * 100, avg["crit_on_time"] * 100, avg["lat"],
                avg["overhead"] * 100, avg["late"]), flush=True)
            assert avg["bad"] == 0, "parity recovery produced wrong data"
    json.dump(out, open("results.json", "w"), indent=1)
