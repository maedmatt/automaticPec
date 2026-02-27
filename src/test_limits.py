"""Test Aruba PEC sending limits incrementally.

Run each test one at a time, check you're not blocked between tests.
Usage:
    uv run python src/test_limits.py 1   # Test 1: single email with attachment
    uv run python src/test_limits.py 2   # Test 2: 2 emails sequential, 1 connection
    uv run python src/test_limits.py 3   # Test 3: 2 emails parallel, 2 connections
    uv run python src/test_limits.py 4   # Test 4: 4 emails sequential, 1 connection
"""

from __future__ import annotations

import logging
import os
import smtplib
import sys
import time
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from threading import Event, Thread

from dotenv import load_dotenv

log = logging.getLogger("test")

SERVER = "smtps.pec.aruba.it"
PORT = 465


def ts() -> str:
    from datetime import datetime

    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def build_msg(user: str, to: str, subject: str, pdf: Path | None = None) -> str:
    msg = MIMEMultipart()
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = subject
    msg.attach(MIMEText("Test invio PEC", "plain"))

    if pdf and pdf.exists():
        part = MIMEBase("application", "octet-stream")
        part.set_payload(pdf.read_bytes())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{pdf.name}"')
        msg.attach(part)

    return msg.as_string()


def connect(user: str, pwd: str) -> smtplib.SMTP_SSL:
    s = smtplib.SMTP_SSL(SERVER, PORT, timeout=300, local_hostname=SERVER)
    _ = s.login(user, pwd)
    log.info("connected at %s", ts())
    return s


def test1(user: str, pwd: str) -> None:
    """Single email with attachment — baseline."""
    log.info("=== TEST 1: single email with attachment ===")
    pdf = Path("files/2026/DocumentoIdentita_MatteoCalabria.pdf")
    msg = build_msg(user, user, "Test 1 - singolo con allegato", pdf)
    log.info("message size: %d bytes", len(msg))

    s = connect(user, pwd)
    t0 = time.perf_counter()
    s.sendmail(user, user, msg)
    dt = time.perf_counter() - t0
    log.info("sent at %s (%.3fs)", ts(), dt)
    _ = s.quit()


def test2(user: str, pwd: str) -> None:
    """2 emails sequential on 1 connection."""
    log.info("=== TEST 2: 2 emails sequential, 1 connection ===")
    pdf = Path("files/2026/DocumentoIdentita_MatteoCalabria.pdf")
    msgs = [
        build_msg(user, user, "Test 2a - sequenziale", pdf),
        build_msg(user, user, "Test 2b - sequenziale", pdf),
    ]

    s = connect(user, pwd)
    for i, msg in enumerate(msgs):
        t0 = time.perf_counter()
        s.sendmail(user, user, msg)
        dt = time.perf_counter() - t0
        log.info("email %d sent at %s (%.3fs)", i + 1, ts(), dt)
    _ = s.quit()


def test3(user: str, pwd: str) -> None:
    """2 emails parallel on 2 connections."""
    log.info("=== TEST 3: 2 emails parallel, 2 connections ===")
    pdf = Path("files/2026/DocumentoIdentita_MatteoCalabria.pdf")
    msgs = [
        build_msg(user, user, "Test 3a - parallelo", pdf),
        build_msg(user, user, "Test 3b - parallelo", pdf),
    ]

    conns = [connect(user, pwd), connect(user, pwd)]
    fire = Event()

    def send(s: smtplib.SMTP_SSL, msg: str, idx: int) -> None:
        _ = fire.wait()
        t0 = time.perf_counter()
        s.sendmail(user, user, msg)
        dt = time.perf_counter() - t0
        log.info("email %d sent at %s (%.3fs)", idx, ts(), dt)

    threads = [Thread(target=send, args=(conns[i], msgs[i], i + 1)) for i in range(2)]
    for t in threads:
        t.start()
    fire.set()
    for t in threads:
        t.join()
    for c in conns:
        _ = c.quit()


def test4(user: str, pwd: str) -> None:
    """4 emails sequential on 1 connection."""
    log.info("=== TEST 4: 4 emails sequential, 1 connection ===")
    pdf = Path("files/2026/DocumentoIdentita_MatteoCalabria.pdf")
    msgs = [build_msg(user, user, f"Test 4{c} - sequenziale", pdf) for c in "abcd"]

    s = connect(user, pwd)
    for i, msg in enumerate(msgs):
        t0 = time.perf_counter()
        s.sendmail(user, user, msg)
        dt = time.perf_counter() - t0
        log.info("email %d sent at %s (%.3fs)", i + 1, ts(), dt)
    _ = s.quit()


TESTS = {"1": test1, "2": test2, "3": test3, "4": test4}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 2 or sys.argv[1] not in TESTS:
        print(f"Usage: {sys.argv[0]} [{'/'.join(TESTS)}]")
        sys.exit(1)

    _ = load_dotenv()
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASSWORD")
    if not user or not pwd:
        log.error("SMTP_USER and SMTP_PASSWORD must be set in .env")
        sys.exit(1)

    TESTS[sys.argv[1]](user, pwd)

    # Verify we're not blocked after the test
    log.info("--- verifying connection still works ---")
    try:
        s = connect(user, pwd)
        _ = s.quit()
        log.info("OK — not blocked")
    except Exception as e:
        log.error("BLOCKED: %s", e)


if __name__ == "__main__":
    main()
