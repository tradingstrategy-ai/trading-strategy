from typing import Optional

from capitalgram.environment.base import download_with_progress_plain
from capitalgram.environment.config import Configuration
from capitalgram.transport.cache import CachedHTTPTransport


def run_interactive_setup() -> Optional[Configuration]:
    """Do REPL interactive setup with the user in Jupyter notebook."""

    api_key = None
    transport = CachedHTTPTransport(download_with_progress_plain, api_key=None)

    # Check the server is up - will throw an exception in the case of an error
    transport.ping()

    has_api_key = None
    while has_api_key not in ("y", "n", ""):
        print("Using Capitalgram requires you to have an API key.")
        has_api_key = input("Do you have an API key yet? [y/n]").lower()

    if has_api_key == "":
        print("Aborting setup")
        return None

    if has_api_key == "y":
        pass
    else:
        print("The Professional account and API key costs $990, but a FREE API key is available for those who sign up on the mailing list during the private beta.")
        reply = input("Would you like to sign up on the mailing list and get a free API key now? [y/n]").lower()
        if reply != 'y':
            print("Thank you. See you next time.")
            return None

        first_name = input("Your first name")
        last_name = input("Your last name")
        email = input("Valid email address - the API key will be sent to this email address")

        resp = transport.register(first_name, last_name, email)
        if resp["status"] != "OK":
            print(f"Failed to register: {resp}")
            return None

        print(f"Signed up on the newsletter: {email} - please check your email for the API key")

    valid_api_key = False
    while not valid_api_key:
        api_key = input("Enter your API key from the welcome email")
        api_key = api_key.strip()  # Watch out whitespace copy paste issues
        if api_key == "":
            print("Aborting setup")
            return None

        print(f"Testing out API key: {api_key[0:6]}...")
        authenticated_transport = CachedHTTPTransport(api_key=api_key)
        try:
            welcome = authenticated_transport.message_of_the_day()
            print("The server replied accepted our API key and sent the following greetings:")
            print("Server version:", welcome["version"])
            print("Message of the day:", welcome["message"])
        except Exception as e:
            print(f"Received error: {e} - check your API key")
            continue

        valid_api_key = True

    config = Configuration(api_key=api_key)

    print("Setting up the new API complete.")

    return config






