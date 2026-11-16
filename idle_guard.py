"""Local CLI/panel idle lifecycle guard. No cloud, signals, or resident service."""
import contextlib
import fcntl
import hashlib
import json
import math
import os
import subprocess
import sys
import tempfile
import time
import uuid


class IdleError(RuntimeError):
    pass


def identity(pid=None):
    pid = os.getpid() if pid is None else int(pid)
    try:
        # comm can contain spaces and ')'; fields after the last ')' start at 3.
        fields = open('/proc/%d/stat' % pid).read().rsplit(')', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return [pid, fields[19]]
    except (OSError, ValueError, IndexError):
        return None


class Lease:
    def __init__(self, guard, fd, generation):
        self.guard, self.fd, self.generation = guard, fd, generation

    def close(self):
        if self.fd is None:
            return
        fd, self.fd = self.fd, None
        try:
            with self.guard.gate():
                # Release only under the same decision gate used to admit work.
                os.close(fd)
                fd = None
                data = self.guard.read(repair=True)
                data['last'] = max(data['last'], self.guard.now())
                data['busy'] = self.guard.busy()
                self.guard.write(data)
        finally:
            if fd is not None:
                os.close(fd)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Guard:
    def __init__(self, state_dir, session):
        self.root, self.session = state_dir, session
        self.key = hashlib.sha256(session.encode()).hexdigest()
        self.prefix = os.path.join(state_dir, 'idle-' + self.key)
        self.path = self.prefix + '.json'
        self.genpath = os.path.join(state_dir, 'power-generation-' + self.key)

    def now(self):
        value = time.monotonic()
        if not math.isfinite(value) or value < 0:
            raise IdleError('invalid monotonic clock; idle shutdown inhibited')
        return value

    def boot(self):
        return open('/proc/sys/kernel/random/boot_id').read().strip()

    def generation(self):
        try:
            return open(self.genpath).read().strip()
        except FileNotFoundError:
            return '0'

    @contextlib.contextmanager
    def gate(self):
        os.makedirs(self.root, mode=0o700, exist_ok=True)
        with open(self.prefix + '.lock', 'a+') as fh:
            fcntl.flock(fh, fcntl.LOCK_EX)
            yield

    def atomic(self, path, data):
        fd, tmp = tempfile.mkstemp(prefix='.idle-', dir=self.root)
        try:
            with os.fdopen(fd, 'w') as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            dfd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def write(self, data):
        self.atomic(self.path, json.dumps(data, allow_nan=False))

    def read(self, repair=False):
        try:
            data = json.load(open(self.path))
            last = data['last']
            limit, retry = data.get('limit', 1800), data.get('retry_at', 0)
            attempts = data.get('attempts', 0)
            if (any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v)
                    for v in (limit, retry)) or retry < 0
                    or type(attempts) is not int or not 0 <= attempts <= 3):
                raise ValueError('invalid idle retry/limit state')
            if (data['boot'] != self.boot() or data['generation'] != self.generation()
                    or isinstance(last, bool) or not isinstance(last, (float, int))
                    or not math.isfinite(last) or not 0 <= last <= self.now()
                    or data['phase'] not in ('running', 'stopping', 'failed', 'stopped')
                    or type(data['busy']) is not bool):
                raise ValueError('invalid idle state')
            return data
        except (OSError, ValueError, KeyError, TypeError) as exc:
            if not repair:
                raise IdleError('idle state missing/invalid; shutdown inhibited until real activity') from exc
            # Only explicit real activity/start may establish a new baseline, not polling.
            return dict(boot=self.boot(), generation=self.generation(), last=self.now(),
                        phase='running', busy=False, error=None, attempts=0, retry_at=0)

    def touch(self):
        with self.gate():
            data = self.read(repair=True)
            data['last'] = max(data['last'], self.now())
            self.write(data)

    def busy(self):
        fd = os.open(self.prefix + '.busy', os.O_RDWR | os.O_CREAT, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return False
            except BlockingIOError:
                return True
        finally:
            os.close(fd)

    def lease(self, expected=None):
        with self.gate():
            if expected is not None and expected != self.generation():
                raise IdleError('old generation fenced; manual continue required')
            data = self.read(repair=True)
            if data['phase'] in ('stopping', 'failed'):
                raise IdleError('shutdown incomplete; retry manual stop before new work')
            fd = os.open(self.prefix + '.busy', os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_SH)
                data.update(busy=True, phase='running', error=None)
                data['last'] = max(data['last'], self.now())
                self.write(data)
            except BaseException:
                os.close(fd)
                raise
            return Lease(self, fd, data['generation'])

    def claim_stop(self, expected=None, automatic=False, limit=1800):
        with self.gate():
            current = self.generation()
            if expected is not None and expected != current:
                raise IdleError('old stop generation fenced')
            data = self.read(repair=not automatic)
            if automatic:
                if data['phase'] != 'running' or self.busy():
                    return None
                if data['busy']:
                    # An abruptly killed worker released the kernel lease. Start the
                    # idle period at detection, never pretend it finished at admission.
                    data.update(busy=False, last=self.now())
                    self.write(data)
                    return None
                if self.now() - data['last'] < limit:
                    return None
            elif data['phase'] == 'stopping':
                if expected is not None:
                    return current  # panel has already fenced/paused this stop
                raise IdleError('shutdown already in progress')
            token = uuid.uuid4().hex
            self.atomic(self.genpath, token)
            data.update(generation=token, phase='stopping', attempts=0,
                        retry_at=0, stop_owner=None, error=None)
            self.write(data)
            return token

    def begin_attempt(self, token):
        with self.gate():
            if token != self.generation():
                raise IdleError('obsolete shutdown claim')
            data = self.read()
            if data['phase'] == 'stopped':
                return False
            if data['phase'] not in ('stopping', 'failed'):
                raise IdleError('shutdown claim retired by new work')
            if data.get('stop_owner') and identity(data['stop_owner'][0]) == data['stop_owner']:
                raise IdleError('shutdown attempt already active')
            if data.get('attempts', 0) >= 3:
                raise IdleError('automatic retry budget exhausted; manual stop required')
            data.update(phase='stopping', stop_owner=identity(), attempts=data.get('attempts', 0) + 1)
            self.write(data)
            return True

    def finish_stop(self, token, success, error=None):
        with self.gate():
            if token != self.generation():
                return
            data = self.read()
            data.update(phase='stopped' if success else 'failed', stop_owner=None,
                        error=None if success else (error or 'shutdown not confirmed'),
                        retry_at=self.now() + (60 if data.get('attempts', 0) == 1 else 300))
            self.write(data)

    def retry_due(self, generation):
        with self.gate():
            if generation != self.generation():
                return False
            data = self.read()
            if data['phase'] not in ('stopping', 'failed') or data.get('attempts', 0) >= 3:
                return False
            owner = data.get('stop_owner')
            return (not owner or identity(owner[0]) != owner) and self.now() >= data.get('retry_at', 0)

    def ensure_watcher(self, cli, minutes):
        # Stable lock inode per generation; no PID is ever signalled/unlinked.
        with self.gate():
            data = self.read()
            data['limit'] = minutes * 60
            self.write(data)
            if minutes <= 0 or data['phase'] == 'stopped':
                return
            generation = self.generation()
            suffix = hashlib.sha256(generation.encode()).hexdigest()
            fd = os.open(self.prefix + '.watch-' + suffix + '.lock', os.O_RDWR | os.O_CREAT, 0o600)
            try:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return
                with open(self.prefix + '.log', 'a') as log:
                    subprocess.Popen([sys.executable, cli, '_idle_watch', self.session,
                                      generation, str(fd)], pass_fds=(fd,),
                                     stdout=subprocess.DEVNULL,
                                     stderr=log, start_new_session=True)
            finally:
                # Do not LOCK_UN: the child inherits this open-file-description lock.
                os.close(fd)

    def watch(self, generation, fd, stop, keepalive=None, keepalive_interval=300):
        owner = identity()
        try:
            with self.gate():
                if generation != self.generation():
                    return
                data = self.read()
                data['watcher'] = dict(identity=owner, generation=generation)
                self.write(data)
            previous_error = None
            last_keepalive = 0.0
            while True:
                try:
                    with self.gate():
                        if generation != self.generation():
                            return
                        data = self.read()
                        limit = data.get('limit', 1800)
                        if limit <= 0 or data['phase'] == 'stopped':
                            return
                    if self.retry_due(generation):
                        stop(generation)
                    elif data['phase'] == 'running':
                        token = self.claim_stop(expected=generation, automatic=True, limit=limit)
                        if token:
                            stop(token)
                            return
                        if keepalive is not None and self.now() - data['last'] < limit:
                            now_mono = self.now()
                            if last_keepalive == 0.0 or now_mono - last_keepalive >= keepalive_interval:
                                last_keepalive = now_mono
                                try:
                                    keepalive(generation)
                                except (Exception, SystemExit) as kexc:
                                    print('[idle-keepalive] ' + type(kexc).__name__ + ': ' + str(kexc), file=sys.stderr, flush=True)
                    previous_error = None
                except (Exception, SystemExit) as exc:
                    # Fail visibly and closed, without refreshing timestamps or cloud polling.
                    message = type(exc).__name__ + ': ' + str(exc)
                    if message != previous_error:
                        print('[idle] ' + message, file=sys.stderr, flush=True)
                        previous_error = message
                time.sleep(30)
        finally:
            try:
                with self.gate():
                    data = self.read()
                    if data.get('watcher') == dict(identity=owner, generation=generation):
                        data.pop('watcher', None)
                        self.write(data)
            except Exception:
                pass
            os.close(fd)
