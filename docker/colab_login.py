#!/usr/bin/env python3
"""Persistent local OAuth login using the installed upstream Colab auth provider."""
import argparse
import os


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--client-oauth-config', help='Optional OAuth client JSON path')
    args = parser.parse_args()
    os.umask(0o077)
    from colab_cli.auth import get_credentials, KEYRING_SERVICE_NAME, KEYRING_USERNAME
    import keyring
    with get_credentials(config_path=args.client_oauth_config):
        if not keyring.get_password(KEYRING_SERVICE_NAME, KEYRING_USERNAME):
            raise SystemExit('OAuth credentials were not persisted; check keyring permissions.')
    print('Colab account authorized; credentials persisted in the /data home volume.')


if __name__ == '__main__':
    main()
