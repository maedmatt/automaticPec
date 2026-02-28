"""Send PEC emails at a precise time with pre-warmed SMTP connections.

Strategy: connect ~30s before target, pre-stage MAIL FROM + RCPT TO ~3s
before, then fire only DATA at the exact moment. This reduces the critical
path from 4 to 2 SMTP round-trips without long-lived connections.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import smtplib
import socket
import sys
import threading
import time
import tomllib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import tyro
from dotenv import load_dotenv

log = logging.getLogger("pec")


@dataclasses.dataclass
class Args:
    """Send PEC at precise time."""

    to: str | None = None
    """Override all recipients with this address."""
    now: bool = False
    """Send immediately instead of waiting for target time."""
    config: Path = Path("config.toml")
    """Path to config file."""


def build_message(
    from_addr: str,
    to_addr: str,
    subject: str,
    body: str,
    attachments: list[Path],
) -> str:
    """Pre-build the full MIME message as a string, ready for SMTP DATA."""
    msg = MIMEMultipart()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for path in attachments:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(path.read_bytes())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{path.name}"')
        msg.attach(part)

    return msg.as_string()


class Connection:
    """SMTP_SSL connection for pre-staged sending."""

    server: str
    port: int
    user: str
    password: str
    smtp: smtplib.SMTP_SSL | None

    def __init__(self, server: str, port: int, user: str, password: str):
        self.server = server
        self.port = port
        self.user = user
        self.password = password
        self.smtp = None

    def _s(self) -> smtplib.SMTP_SSL:
        assert self.smtp is not None
        return self.smtp

    def connect(self) -> None:
        self.smtp = smtplib.SMTP_SSL(
            self.server, self.port, timeout=300, local_hostname=self.server
        )
        s = self._s()
        _ = s.login(self.user, self.password)
        assert s.sock is not None
        s.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        log.info("connected and authenticated")

    def pre_stage(self, to_addr: str) -> None:
        """Send MAIL FROM + RCPT TO — the first 2 of 4 round-trips."""
        s = self._s()
        code, resp = s.mail(self.user)
        if code != 250:
            raise smtplib.SMTPResponseException(code, resp)
        code, resp = s.rcpt(to_addr)
        if code != 250:
            raise smtplib.SMTPResponseException(code, resp)

    def send_data(self, message: str) -> tuple[int, bytes]:
        """Send DATA — the remaining 2 round-trips. This is the time-critical call."""
        return self._s().data(message.encode("ascii"))

    def quit(self) -> None:
        if self.smtp:
            try:
                _ = self.smtp.quit()
            except Exception:
                pass


def spin_wait(target: float) -> None:
    """Wait until wall-clock target with sub-ms precision.

    Uses time.time() for coarse sleep (it tracks wall-clock / UTC), then
    switches to time.perf_counter() for the final 50ms spin to avoid any
    clock adjustment jitter in the critical window.
    """
    while True:
        remaining = target - time.time()
        if remaining <= 0:
            return
        if remaining > 0.05:
            time.sleep(remaining - 0.05)
            continue
        # Final 50ms: spin on perf_counter to avoid NTP step adjustments
        deadline = time.perf_counter() + (target - time.time())
        while time.perf_counter() < deadline:
            pass
        return


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def main() -> None:
    args: Args = tyro.cli(Args)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
    )

    _ = load_dotenv()
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    if not smtp_user or not smtp_password:
        log.error("SMTP_USER and SMTP_PASSWORD must be set in .env")
        sys.exit(1)

    config: dict[str, Any] = tomllib.loads(args.config.read_text())
    settings: dict[str, Any] = config["settings"]
    target_time = datetime.fromisoformat(str(settings["target_time"]))
    target_ts = target_time.timestamp()
    files_dir = Path(str(settings["files_dir"]))
    body = str(settings["body"])

    # Phase 0: build all messages and verify attachments
    email_entries: list[dict[str, Any]] = config["emails"]
    emails: list[tuple[str, str, str]] = []  # (recipient, subject, message_str)
    for entry in email_entries:
        recipient = args.to if args.to else str(entry["recipient"])
        attachment_names: list[str] = [str(f) for f in entry["attachments"]]
        attachments = [files_dir / f for f in attachment_names]

        for a in attachments:
            if not a.exists():
                log.error("missing attachment: %s", a)
                sys.exit(1)

        subject = str(entry["subject"])
        message = build_message(smtp_user, recipient, subject, body, attachments)
        emails.append((recipient, subject, message))
        log.info("built: %s → %s (%d bytes)", subject, recipient, len(message))

    # Phase 1: prepare connection objects
    conns: list[tuple[Connection, str, str, str]] = []
    for recipient, subject, message in emails:
        conn = Connection(
            str(settings["smtp_server"]),
            int(settings["smtp_port"]),
            smtp_user,
            smtp_password,
        )
        conns.append((conn, recipient, subject, message))

    if not args.now:
        remaining = target_ts - time.time()
        if remaining < 0:
            log.error("target time %s is in the past", target_time)
            sys.exit(1)
        log.info("waiting %.1fs until %s", remaining, target_time)

        # Heartbeat every 5 min until T-30s
        connect_at = target_ts - 30.0
        while time.time() < connect_at:
            left = target_ts - time.time()
            if left <= 30.0:
                break
            hours = int(left // 3600)
            mins = int((left % 3600) // 60)
            log.info(
                "%dh %02dm left — firing at %s",
                hours,
                mins,
                target_time.strftime("%H:%M:%S"),
            )
            time.sleep(min(300.0, left - 30.0))

    # Phase 2: connect + authenticate (~0.5s per connection)
    for conn, _, subject, _ in conns:
        conn.connect()
    log.info("%d connections ready at %s", len(conns), ts())

    if not args.now:
        # Pre-stage MAIL FROM + RCPT TO ~3s before target
        spin_wait(target_ts - 3.0)

    for conn, recipient, subject, _ in conns:
        conn.pre_stage(recipient)
        log.info("pre-staged: %s at %s", subject, ts())

    # Phase 3: fire all DATA simultaneously at target time
    fire = threading.Event()
    results: dict[str, tuple[str, str]] = {}

    def worker(c: Connection, subj: str, msg: str) -> None:
        _ = fire.wait()
        t0 = time.perf_counter()
        try:
            code, resp = c.send_data(msg)
            dt = time.perf_counter() - t0
            if code == 250:
                log.info("SENT %s at %s (%.3fs)", subj, ts(), dt)
                results[subj] = ("ok", f"{dt:.3f}s")
            else:
                log.error("REJECTED %s: %d %s (%.3fs)", subj, code, resp, dt)
                results[subj] = ("rejected", f"{code} {resp}")
        except Exception as e:
            dt = time.perf_counter() - t0
            log.error("FAILED %s: %s (%.3fs)", subj, e, dt)
            results[subj] = ("error", str(e))

    threads: list[threading.Thread] = []
    for conn, _, subject, message in conns:
        t = threading.Thread(target=worker, args=(conn, subject, message))
        t.start()
        threads.append(t)

    if not args.now:
        spin_wait(target_ts)

    log.info("firing DATA at %s", ts())
    fire.set()

    for t in threads:
        t.join(timeout=120)

    for conn, _, _, _ in conns:
        conn.quit()

    log.info("--- results ---")
    for subject, (status, detail) in results.items():
        log.info("  %s: %s (%s)", subject, status, detail)


if __name__ == "__main__":
    main()
