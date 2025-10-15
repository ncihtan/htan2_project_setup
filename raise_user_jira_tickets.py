#!/usr/bin/env python3
"""Create Jira Service Management requests from a simple email-like input file.

Each request is created on behalf of the email owner, and the provided body is
posted as the first public comment. The CLI uses rich-click for colourful help
output and supports a dry-run mode for verification.
"""

# UX note:
# - To remove Jira’s “X commented:” wrapper in customer emails, edit the
#   JSM project's “Public comment added” customer notification template
#   to only include ${comment.body}. This script focuses on clean subjects,
#   optional intro/footers, and consistent tone; template changes finish the job.

from __future__ import annotations

import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import dotenv
import requests
import rich_click as click

dotenv.load_dotenv()

logger = logging.getLogger(__name__)

click.rich_click.MAX_WIDTH = 100
click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.USE_MARKDOWN = True

EMAIL_RE = re.compile(r"^[\w\.\+\-]+@[\w\.\-]+\.\w+$")
DEFAULT_PLACEHOLDER = "Tracking ticket created by HTAN DCC automation."

MAX_SUBJECT_LEN = 60

def _normalize_subject(prefix: str, subject: str) -> str:
    s = f"{prefix}{subject}" if prefix else subject
    return s[:MAX_SUBJECT_LEN]

def format_comment(body: str, intro: Optional[str], footer: Optional[str]) -> str:
    parts: List[str] = []
    if intro:
        parts.append(intro.strip())
        parts.append("")  # blank line
    parts.append(body.strip())
    if footer:
        parts.append("")
        parts.append(footer.strip())
    return "\n".join(parts)


class MissingConfigurationError(RuntimeError):
    """Raised when required configuration is not provided."""


def parse_blocks(text: str) -> List[Tuple[str, str, str]]:
    """Parse the email/subject/body format described in the module docstring."""
    lines = [line.rstrip("\n") for line in text.splitlines()]
    blocks: List[Tuple[str, str, str]] = []
    i, n = 0, len(lines)
    while i < n:
        while i < n:
            candidate = lines[i].strip()
            if candidate and not candidate.startswith("#") and EMAIL_RE.match(candidate):
                break
            i += 1
        if i >= n:
            break
        email = lines[i].strip()
        i += 1

        while i < n and (not lines[i].strip() or lines[i].strip().startswith("#")):
            i += 1
        if i >= n:
            break
        subject = lines[i].strip()
        i += 1

        body_lines: List[str] = []
        while i < n and not EMAIL_RE.match(lines[i].strip()):
            body_lines.append(lines[i])
            i += 1
        body = "\n".join(body_lines).strip()
        blocks.append((email, subject, body))
    return blocks


@dataclass
class JiraConfig:
    jira_url: str
    jira_email: str
    jira_api_token: str
    service_desk_id: str
    request_type_id: str
    request_participants: Sequence[str]
    subject_prefix: str = ""
    intro_line: Optional[str] = None
    footer_line: Optional[str] = None
    placeholder_description: str = DEFAULT_PLACEHOLDER
    dry_run: bool = False
    delay_seconds: float = 0.0
    max_requests: Optional[int] = None
    request_timeout: float = 30.0
    ensure_customer: bool = True
    assign_to_creator: bool = True

    def validate(self) -> None:
        missing = [
            name
            for name, value in (
                ("JIRA_URL", self.jira_url),
                ("JIRA_EMAIL", self.jira_email),
                ("JIRA_API_TOKEN", self.jira_api_token),
                ("SERVICE_DESK_ID", self.service_desk_id),
                ("REQUEST_TYPE_ID", self.request_type_id),
            )
            if not value
        ]
        if missing:
            raise MissingConfigurationError(
                "Missing required configuration: " + ", ".join(missing)
            )


class JiraClient:
    """Thin wrapper around the Jira Service Management REST API."""

    def __init__(self, config: JiraConfig):
        self._config = config
        self._session = requests.Session()
        self._session.auth = (config.jira_email, config.jira_api_token)
        self._session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        self._creator_account_id: Optional[str] = None

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "JiraClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _retry_request(self, method: str, url: str, expected: Iterable[int], **kwargs):
        """Simple retry for 429/5xx with backoff."""
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(6):
            try:
                response = self._session.request(
                    method,
                    url,
                    timeout=self._config.request_timeout,
                    **kwargs,
                )
            except requests.RequestException as exc:
                last_exc = exc
                logger.warning("Request error (%s %s): %s", method, url, exc)
            else:
                if response.status_code in expected:
                    return response
                if response.status_code in (429,) or 500 <= response.status_code < 600:
                    logger.info(
                        "Retrying %s %s after status %s (attempt %d)",
                        method,
                        url,
                        response.status_code,
                        attempt + 1,
                    )
                else:
                    return response
            time.sleep(backoff)
            backoff = min(backoff * 2, 16)
        if last_exc:
            raise last_exc
        return response

    def _get_creator_account_id(self) -> str:
        if self._creator_account_id is not None:
            return self._creator_account_id
        url = f"{self._config.jira_url}/rest/api/3/myself"
        response = self._retry_request("GET", url, expected=(200,))
        if response.status_code != 200:
            raise RuntimeError(
                f"Failed to resolve creator accountId: "
                f"{response.status_code} {response.text}"
            )
        data = response.json()
        account_id = data.get("accountId")
        if not account_id:
            raise RuntimeError(f"No accountId in response: {data}")
        self._creator_account_id = account_id
        return account_id

    def ensure_customer(self, email: str) -> None:
        if not self._config.ensure_customer:
            return
        if self._config.dry_run:
            logger.debug("[dry-run] Skipping ensure_customer for %s", email)
            return
        url = f"{self._config.jira_url}/rest/servicedeskapi/customer"
        payload = {"email": email, "displayName": email.split("@")[0]}
        response = self._retry_request("POST", url, expected=(201, 400, 409), json=payload)
        if response.status_code not in (201, 400, 409):
            logger.warning(
                "Could not create/find customer %s: %s %s",
                email,
                response.status_code,
                response.text,
            )

    def create_request_on_behalf(self, requester_email: str, summary: str) -> str:
        summary = _normalize_subject(self._config.subject_prefix, summary)
        if self._config.dry_run:
            logger.info("[dry-run] Would create request for %s", requester_email)
            return "DRY-RUN"

        url = f"{self._config.jira_url}/rest/servicedeskapi/request"
        payload = {
            "serviceDeskId": self._config.service_desk_id,
            "requestTypeId": self._config.request_type_id,
            "requestFieldValues": {
                "summary": summary,
                "description": self._config.placeholder_description,
            },
            "raiseOnBehalfOf": requester_email,
        }
        if self._config.request_participants:
            payload["requestParticipants"] = list(self._config.request_participants)
        response = self._retry_request("POST", url, expected=(201,), json=payload)
        if response.status_code != 201:
            raise RuntimeError(
                f"Create request failed for {requester_email}: "
                f"{response.status_code} {response.text}"
            )
        data = response.json()
        issue_key = data.get("issueKey") or data.get("key")
        if not issue_key:
            raise RuntimeError(
                f"Create request succeeded but no issue key returned: {data}"
            )
        return issue_key

    def add_public_comment(self, issue_key: str, body_text: str) -> None:
        body_text = format_comment(body_text, self._config.intro_line, self._config.footer_line)
        if self._config.dry_run:
            logger.info("[dry-run] Would add public comment to %s", issue_key)
            return
        url = f"{self._config.jira_url}/rest/servicedeskapi/request/{issue_key}/comment"
        payload = {"public": True, "body": body_text}
        response = self._retry_request("POST", url, expected=(201,), json=payload)
        if response.status_code != 201:
            raise RuntimeError(
                f"Add comment failed for {issue_key}: "
                f"{response.status_code} {response.text}"
            )

    def assign_to_creator(self, issue_key: str) -> None:
        if not self._config.assign_to_creator:
            return
        if self._config.dry_run:
            logger.info("[dry-run] Would assign %s to creator", issue_key)
            return
        account_id = self._get_creator_account_id()
        url = f"{self._config.jira_url}/rest/api/3/issue/{issue_key}/assignee"
        payload = {"accountId": account_id}
        response = self._retry_request("PUT", url, expected=(204,), json=payload)
        if response.status_code != 204:
            raise RuntimeError(
                f"Assign issue failed for {issue_key}: "
                f"{response.status_code} {response.text}"
            )


def split_participants(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def build_config(
    *,
    jira_url: Optional[str],
    jira_email: Optional[str],
    jira_api_token: Optional[str],
    service_desk_id: Optional[str],
    request_type_id: Optional[str],
    participants: Optional[str],
    placeholder: Optional[str],
    dry_run: bool,
    delay: float,
    max_requests: Optional[int],
    timeout: float,
    skip_customer: bool,
    assign_to_creator: bool,
    subject_prefix: Optional[str],
    intro_line: Optional[str],
    footer_line: Optional[str],
) -> JiraConfig:
    config = JiraConfig(
        jira_url=(jira_url or "").rstrip("/"),
        jira_email=jira_email or "",
        jira_api_token=jira_api_token or "",
        service_desk_id=service_desk_id or "",
        request_type_id=request_type_id or "",
        request_participants=split_participants(participants),
        subject_prefix=subject_prefix or "",
        intro_line=intro_line,
        footer_line=footer_line,
        placeholder_description=placeholder or DEFAULT_PLACEHOLDER,
        dry_run=dry_run,
        delay_seconds=delay,
        max_requests=max_requests,
        request_timeout=timeout,
        ensure_customer=not skip_customer,
        assign_to_creator=assign_to_creator,
    )
    config.validate()
    return config


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("input_file", type=click.Path(path_type=Path, exists=True, dir_okay=False))
@click.option(
    "--jira-url",
    envvar="JIRA_URL",
    metavar="URL",
    help="Jira base URL.",
)
@click.option(
    "--jira-email",
    envvar="JIRA_EMAIL",
    metavar="EMAIL",
    help="Agent email address.",
)
@click.option(
    "--jira-api-token",
    envvar="JIRA_API_TOKEN",
    metavar="TOKEN",
    help="API token for the agent.",
)
@click.option(
    "--service-desk-id",
    envvar="SERVICE_DESK_ID",
    metavar="ID",
    help="Service desk identifier.",
)
@click.option(
    "--request-type-id",
    envvar="REQUEST_TYPE_ID",
    metavar="ID",
    help="Request type identifier.",
)
@click.option(
    "--participants",
    envvar="ADD_PARTICIPANTS",
    metavar="EMAILS",
    help="Comma-separated request participant emails.",
)
@click.option(
    "--placeholder",
    metavar="TEXT",
    default=DEFAULT_PLACEHOLDER,
    show_default=True,
    help="Placeholder description for the Jira issue.",
)
@click.option(
    "--subject-prefix",
    envvar="SUBJECT_PREFIX",
    metavar="TEXT",
    help="Prefix to add to each email subject (e.g., 'HTAN – ').",
)
@click.option(
    "--intro-line",
    envvar="INTRO_LINE",
    metavar="TEXT",
    help="Optional line inserted above the body to set tone/context.",
)
@click.option(
    "--footer-line",
    envvar="FOOTER_LINE",
    metavar="TEXT",
    help="Optional line appended below the body (e.g., contact or signature).",
)
@click.option(
    "--delay",
    type=float,
    default=0.0,
    show_default=True,
    help="Seconds to sleep between requests.",
)
@click.option(
    "--timeout",
    type=float,
    default=30.0,
    show_default=True,
    help="HTTP request timeout in seconds.",
)
@click.option(
    "--max-requests",
    type=int,
    metavar="N",
    help="Limit the number of requests processed.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Print actions without performing API calls.",
)
@click.option(
    "--skip-customer",
    is_flag=True,
    help="Do not attempt to create/find the customer before raising the request.",
)
@click.option(
    "--assign-to-creator/--keep-assignee",
    default=True,
    help="Assign created issues to the authenticated agent.",
)
@click.option(
    "--verbose",
    is_flag=True,
    help="Enable debug logging.",
)
def cli(
    input_file: Path,
    jira_url: Optional[str],
    jira_email: Optional[str],
    jira_api_token: Optional[str],
    service_desk_id: Optional[str],
    request_type_id: Optional[str],
    participants: Optional[str],
    placeholder: str,
    subject_prefix: Optional[str],
    intro_line: Optional[str],
    footer_line: Optional[str],
    delay: float,
    timeout: float,
    max_requests: Optional[int],
    dry_run: bool,
    skip_customer: bool,
    assign_to_creator: bool,
    verbose: bool,
) -> None:
    """Create Jira Service Management requests from INPUT_FILE."""
    configure_logging(verbose)
    try:
        config = build_config(
            jira_url=jira_url,
            jira_email=jira_email,
            jira_api_token=jira_api_token,
            service_desk_id=service_desk_id,
            request_type_id=request_type_id,
            participants=participants,
            placeholder=placeholder,
            dry_run=dry_run,
            delay=delay,
            max_requests=max_requests,
            timeout=timeout,
            skip_customer=skip_customer,
            assign_to_creator=assign_to_creator,
            subject_prefix=subject_prefix,
            intro_line=intro_line,
            footer_line=footer_line,
        )
    except MissingConfigurationError as exc:
        raise click.UsageError(str(exc)) from exc

    try:
        content = input_file.read_text(encoding="utf-8")
    except OSError as exc:
        raise click.FileError(str(input_file), hint=str(exc)) from exc

    blocks = parse_blocks(content)
    if not blocks:
        raise click.UsageError(f"No email blocks found in {input_file}")

    if config.max_requests is not None:
        blocks = blocks[: config.max_requests]
    logger.info("Processing %d request(s)", len(blocks))

    with JiraClient(config) as client:
        for index, (email, subject, body) in enumerate(blocks, start=1):
            logger.info("Processing %s (%d/%d)", email, index, len(blocks))
            try:
                client.ensure_customer(email)
                issue_key = client.create_request_on_behalf(email, subject)
                client.add_public_comment(issue_key, body or _normalize_subject(config.subject_prefix, subject))
                client.assign_to_creator(issue_key)
                logger.info("Created %s for %s", issue_key, email)
            except Exception as exc:
                logger.error("Failed to process %s: %s", email, exc)
            if config.delay_seconds:
                time.sleep(config.delay_seconds)


if __name__ == "__main__":
    try:
        cli(standalone_mode=True)
    except click.ClickException as exc:
        exc.show()
        sys.exit(exc.exit_code)
