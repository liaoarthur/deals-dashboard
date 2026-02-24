#!/usr/bin/env python3
"""
Generate a password hash for the AUTH_USERS environment variable.

Usage:
    python generate_password.py <password>
    python generate_password.py              # prompts for password

The output hash is suitable for the AUTH_USERS env var in Railway:
    AUTH_USERS="admin@co.com:<hash>,user@co.com:<hash>"
"""
import sys
import getpass
from werkzeug.security import generate_password_hash


def main():
    if len(sys.argv) > 1:
        password = sys.argv[1]
    else:
        password = getpass.getpass("Enter password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            print("Error: passwords don't match", file=sys.stderr)
            sys.exit(1)

    pwhash = generate_password_hash(password)
    print(f"\nPassword hash:\n{pwhash}")
    print(f"\nExample AUTH_USERS value:")
    print(f"  user@company.com:{pwhash}")
    print(f"\nSet in Railway:")
    print(f"  AUTH_USERS=user@company.com:{pwhash}")


if __name__ == "__main__":
    main()
