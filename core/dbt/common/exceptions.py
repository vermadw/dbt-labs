from typing import List
import os

from dbt.common.constants import SECRET_ENV_PREFIX


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = str(msg)

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


class DbtInternalError(Exception):
    def __init__(self, msg: str):
        self.stack: List = []
        self.msg = scrub_secrets(msg, env_secrets())

    @property
    def type(self):
        return "Internal"

    def process_stack(self):
        lines = []
        stack = self.stack
        first = True

        if len(stack) > 1:
            lines.append("")

            for item in stack:
                msg = "called by"

                if first:
                    msg = "in"
                    first = False

                lines.append(f"> {msg}")

        return lines

    def __str__(self):
        if hasattr(self.msg, "split"):
            split_msg = self.msg.split("\n")
        else:
            split_msg = str(self.msg).split("\n")

        lines = ["{}".format(self.type + " Error")] + split_msg

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(["  " + line for line in lines[1:]])
