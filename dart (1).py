"""DART - Deadline-Aware Real-time Transport protocol (over UDP).
Sender / Receiver implementation. Modes: 'dart', 'retx' (retransmit-all baseline), 'udp' (plain)."""
import select, socket, struct, time, zlib

HDR = struct.Struct(">BBHIHHBBH")          # 16-byte header
HELLO, HELLO_ACK, DATA, PARITY, NACK, REPORT, FIN = range(1, 8)
F_RETX, F_ACK, F_LAST = 1, 2, 4
PAYLEN, CRIT_EVERY, HOLD_MS = 100, 10, 15


def now_ms():
    return int(time.time() * 1000) & 0xFFFFFFFF


def pack(typ, seq, ts, dl, payload=b"", prio=1, flags=0, gid=0, gsz=0):
    h = HDR.pack((1 << 4) | typ, (prio << 4) | flags, seq, ts, dl, len(payload), gid, gsz, 0)
    ck = zlib.crc32(h[:14] + b"\0\0" + payload) & 0xFFFF
    return h[:14] + struct.pack(">H", ck) + payload


def unpack(b):
    if len(b) < 16:
        return None
    v, pf, seq, ts, dl, ln, gid, gsz, ck = HDR.unpack(b[:16])
    pl = b[16:16 + ln]
    if zlib.crc32(b[:14] + b"\0\0" + pl) & 0xFFFF != ck:
        return None                          # corrupted -> drop
    return dict(type=v & 15, prio=pf >> 4, flags=pf & 15, seq=seq, ts=ts, dl=dl, gid=gid, gsz=gsz, payload=pl)


def prio_of(i):                              # 0 critical, 1 normal, 2 droppable
    return 0 if i % CRIT_EVERY == 0 else (1 if i % 2 == 0 else 2)


def make_payload(i):
    return bytes((i + j) % 256 for j in range(PAYLEN))


def choose_k(p):                             # adaptive parity group size
    return 0 if p < 0.01 else 8 if p < 0.05 else 4 if p < 0.15 else 2


def make_parity(items):
    L = max(len(x) for x in items)
    acc, lx = bytearray(L), 0
    for x in items:
        lx ^= len(x)
        for i, b in enumerate(x):
            acc[i] ^= b
    return struct.pack(">H", lx) + bytes(acc)


class Sender:
    def __init__(self, mode, dest, n, interval_ms, deadline_ms):
        self.mode, self.dest, self.n = mode, dest, n
        self.interval, self.deadline = interval_ms, deadline_ms
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.rtt, self.p, self.tok = 100.0, 0.0, 0.0
        self.store, self.bytes = {}, 0
        self.st = dict(data=0, retx=0, parity=0, nacks=0, late_skip=0, prio_drop=0)

    def send(self, b):
        self.bytes += len(b)
        self.sock.sendto(b, self.dest)

    def handshake(self):
        for _ in range(15):
            self.send(pack(HELLO, 0, now_ms(), self.deadline,
                           struct.pack(">HHH", self.interval, self.deadline, CRIT_EVERY)))
            end = time.time() + 0.25
            while time.time() < end:
                if select.select([self.sock], [], [], 0.02)[0]:
                    m = unpack(self.sock.recv(4096))
                    if m and m["type"] == HELLO_ACK:
                        self.rtt = float(max(1, now_ms() - m["ts"]))
                        return True
        return False

    def poll(self, timeout):
        r = select.select([self.sock], [], [], timeout)[0]
        while r:
            m = unpack(self.sock.recv(4096))
            if m:
                self.on_msg(m)
            r = select.select([self.sock], [], [], 0)[0]

    def on_msg(self, m):
        if m["type"] == NACK:
            pl = m["payload"]
            for s in struct.unpack(">%dH" % (len(pl) // 2), pl):
                self.st["nacks"] += 1
                self.maybe_retx(s)
        elif m["type"] == REPORT and not m["flags"] & F_ACK:
            loss, rtt = struct.unpack(">HH", m["payload"])
            self.p = 0.7 * self.p + 0.3 * loss / 1000.0       # smoothed loss estimate
            if rtt:
                self.rtt = 0.5 * self.rtt + 0.5 * rtt
            self.send(pack(REPORT, 0, m["ts"], 0, b"", flags=F_ACK))   # echo for RTT measurement

    def maybe_retx(self, s):
        info = self.store.get(s)
        if info is None or self.mode == "udp":
            return
        ts, prio, payload, gid, gsz = info
        if self.mode == "dart":
            if prio != 0 and not (now_ms() + self.rtt / 2 < ts + self.deadline):   # DEADLINE GATE
                self.st["late_skip"] += 1
                return
            if (prio == 2 and self.tok < 1) or (prio == 1 and self.tok < 0):       # PRIORITY DROP
                self.st["prio_drop"] += 1
                return
            self.tok -= 1
        self.send(pack(DATA, s, ts, self.deadline, payload, prio, F_RETX, gid, gsz))
        self.st["retx"] += 1

    def run(self):
        if not self.handshake():
            raise RuntimeError("handshake failed")
        next_t, group, gid, k = time.time(), [], 0, 0
        for i in range(self.n):
            while next_t - time.time() > 0:
                self.poll(min(next_t - time.time(), 0.002))
            next_t += self.interval / 1000.0
            if self.mode == "dart" and not group:
                k = choose_k(self.p)
            prio, ts, pl = prio_of(i), now_ms(), make_payload(i)
            self.store[i] = (ts, prio, pl, gid, k)
            self.send(pack(DATA, i, ts, self.deadline, pl, prio, 0, gid, k))
            self.st["data"] += 1
            self.tok = min(self.tok + 0.6, 10)
            if self.mode == "dart" and k > 0:
                group.append(i)
                if len(group) == k:
                    self.send(pack(PARITY, group[0], now_ms(), self.deadline,
                                   make_parity([self.store[j][2] for j in group]), 0, 0, gid, k))
                    self.st["parity"] += 1
                    self.tok -= 1
                    group, gid = [], (gid + 1) % 256
        end = time.time() + (self.deadline + 2 * self.rtt) / 1000.0
        while time.time() < end:
            self.poll(0.005)
        for _ in range(3):
            self.send(pack(FIN, self.n, now_ms(), 0, b"", 0, F_LAST))


class Receiver:
    def __init__(self, mode):
        self.mode = mode
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.addr = self.sock.getsockname()
        self.peer, self.rtt = None, 100.0
        self.got, self.parity, self.pending, self.skipped = {}, {}, {}, set()
        self.highest, self.low, self.ref = -1, 0, None
        self.interval, self.deadline = 10, 150
        self.orig_cnt = self.win_orig = self.win_hi = 0
        self.recovered = self.bad_recover = 0
        self.nack_sent = 0

    def send(self, b):
        self.sock.sendto(b, self.peer)

    def est(self, s):                        # estimate ts/deadline of a packet we never saw
        rs, rts, rdl = self.ref
        ts = rts + (s - rs) * self.interval
        return ts, ts + rdl

    def try_recover(self, first):
        k, pl = self.parity[first]
        members = range(first, first + k)
        miss = [m for m in members if m not in self.got]
        if len(miss) != 1:
            return
        lx, acc = struct.unpack(">H", pl[:2])[0], bytearray(pl[2:])
        for m in members:
            if m in self.got:
                x = self.got[m][3]
                lx ^= len(x)
                for i, b in enumerate(x):
                    acc[i] ^= b
        s = miss[0]
        data = bytes(acc[:lx])
        ts, dl = self.est(s)
        self.got[s] = (float(now_ms()), ts, dl, data)
        self.recovered += 1
        if data != make_payload(s):
            self.bad_recover += 1
        self.pending.pop(s, None)

    def on_msg(self, m):
        t = now_ms()
        if m["type"] == HELLO:
            self.interval, self.deadline, _ = struct.unpack(">HHH", m["payload"])
            self.send(pack(HELLO_ACK, 0, m["ts"], 0))
        elif m["type"] == DATA:
            s = m["seq"]
            if s not in self.got:
                self.got[s] = (float(t), m["ts"], m["ts"] + m["dl"], m["payload"])
                self.pending.pop(s, None)
                self.skipped.discard(s)
                if not m["flags"] & F_RETX:
                    self.orig_cnt += 1
            if s >= self.highest:
                self.highest, self.ref = s, (s, m["ts"], m["dl"])
            for f in list(self.parity):
                if f <= s < f + self.parity[f][0]:
                    self.try_recover(f)
        elif m["type"] == PARITY:
            self.parity[m["seq"]] = (m["gsz"], m["payload"])
            self.try_recover(m["seq"])
        elif m["type"] == REPORT and m["flags"] & F_ACK:
            self.rtt = 0.5 * self.rtt + 0.5 * max(1, t - m["ts"])
        elif m["type"] == FIN:
            return True

    def tick(self):
        if self.ref is None:
            return
        t = now_ms()
        while self.low in self.got or self.low in self.skipped:
            self.low += 1
        for s in range(self.low, self.highest):
            if s not in self.got and s not in self.skipped:
                self.pending.setdefault(s, [t, 0, 0])
        nacks = []
        for s, (det, last, cnt) in list(self.pending.items()):
            if s in self.got:
                del self.pending[s]
                continue
            if t - det < HOLD_MS:
                continue
            crit = s % CRIT_EVERY == 0
            if self.mode == "udp" or cnt >= (5 if crit else 3) or \
               (self.mode == "dart" and not crit and not (t + self.rtt < self.est(s)[1])):   # DEADLINE GATE
                self.skipped.add(s)
                del self.pending[s]
            elif t - last >= self.rtt + 10:
                nacks.append(s)
                self.pending[s] = [det, t, cnt + 1]
        if nacks:
            self.nack_sent += len(nacks)
            self.send(pack(NACK, 0, t, 0, struct.pack(">%dH" % len(nacks), *nacks)))

    def run(self, hard=60):
        last_rep, end_t, done = time.time(), time.time() + hard, False
        while time.time() < end_t and not done:
            if select.select([self.sock], [], [], 0.005)[0]:
                data, src = self.sock.recvfrom(4096)
                m = unpack(data)
                if m:
                    self.peer = src
                    done = bool(self.on_msg(m))
            if self.peer is None:
                continue
            self.tick()
            if time.time() - last_rep >= 0.25:                 # periodic REPORT
                last_rep = time.time()
                exp = self.highest - self.win_hi
                loss = 0 if exp <= 0 else max(0.0, 1 - (self.orig_cnt - self.win_orig) / exp)
                self.win_hi, self.win_orig = self.highest, self.orig_cnt
                self.send(pack(REPORT, 0, now_ms(), 0, struct.pack(">HH", int(loss * 1000), int(self.rtt))))


def summarize(got, n):
    on = crit = late = 0
    lat = []
    for s in range(n):
        if s in got:
            arr, ts, dl, _ = got[s]
            if arr <= dl:
                on += 1
                crit += (s % CRIT_EVERY == 0)
                lat.append(arr - ts)
            else:
                late += 1
    return dict(on_time=on / n, crit_on_time=crit / len(range(0, n, CRIT_EVERY)),
                late=late, lat=sum(lat) / len(lat) if lat else 0.0)
