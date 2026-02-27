"""Smoke test: single email, no attachments, no threading."""

from __future__ import annotations

import logging
import os
import smtplib
import sys
from email.mime.text import MIMEText

from dotenv import load_dotenv

log = logging.getLogger("smoke")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(message)s",
        datefmt="%H:%M:%S",
    )

    _ = load_dotenv()
    user = os.environ.get("SMTP_USER")
    pwd = os.environ.get("SMTP_PASSWORD")
    if not user or not pwd:
        log.error("SMTP_USER and SMTP_PASSWORD must be set in .env")
        sys.exit(1)

    to = sys.argv[1] if len(sys.argv) > 1 else user

    msg = MIMEText("Smoke test PEC", "plain")
    msg["From"] = user
    msg["To"] = to
    msg["Subject"] = "Smoke test"

    log.info("connecting to smtps.pec.aruba.it:465")
    with smtplib.SMTP_SSL("smtps.pec.aruba.it", 465) as s:
        _ = s.login(user, pwd)
        log.info("authenticated")
        s.sendmail(user, to, msg.as_string())
        log.info("sent to %s", to)


if __name__ == "__main__":
    main()
