from typing import Optional

from tradingstrategy.environment.base import download_with_progress_plain
from tradingstrategy.environment.config import Configuration
from tradingstrategy.transport.cache import CachedHTTPTransport


def run_interactive_setup() -> Optional[Configuration]:
    """Do REPL interactive setup with the user in Jupyter notebook."""

    api_key = None
    transport = CachedHTTPTransport(download_with_progress_plain, api_key=None)

    # Check the server is up - will throw an exception in the case of an error
    transport.ping()

    has_api_key = None
    while has_api_key not in ("y", "n", ""):
        print("Using Trading Strategy requires you to have an API key.")
        has_api_key = input("Do you have an API key yet? [y/n]? ").lower()

    if has_api_key == "":
        print("Aborting setup")
        return None

    if has_api_key == "y":
        pass
    else:
        reply = input("Would you like to sign up on the mailing list and get a free API key now? [y/n]").lower()
        if reply != 'y':
            print("Please rerun choose 'y'` to the prompt next timew.")
            return None

        first_name = input("Your first name? ")
        last_name = input("Your last name? ")
        print("The API key will be delivered to your email inbox.")
        email = input("Your email address? ")

        resp = transport.register(first_name, last_name, email)
        if resp["status"] != "OK":
            print(f"Failed to register: {resp}")
            return None

        print(f"Signed up on the newsletter: {email} - please check your email for the API key")

    while True:
        print("The API key is in format 'secret-token:tradingstrategy-...'")
        api_key = input("Enter your API key from the welcome email you just received, including secret-token: part: ")

        if api_key.strip() == "" or len(api_key) < 10:
            raise AssertionError("Interactive setup aborted")

        if validate_api_key(api_key):
            break

    config = Configuration(api_key=api_key)

    print("The API key setup complete.")

    return config


def run_non_interactive_setup(**kwargs) -> Optional[Configuration]:
    api_key = kwargs.get("api_key")
    if not api_key:
        print("Can't setup config, please provide api_key argument")
        return None
    
    if not validate_api_key(api_key):
        print("Incorrect API key")
        return None

    config = Configuration(api_key=api_key)

    print("The API key setup complete.")

    return config


def validate_api_key(api_key: str) -> bool:
    # Watch out whitespace copy paste issues
    api_key = api_key.strip()  
    if api_key == "":
        print("Aborting setup")
        return False

    print(f"Testing out API key: {api_key[0:24]}")
    authenticated_transport = CachedHTTPTransport(download_with_progress_plain, api_key=api_key)
    try:
        welcome = authenticated_transport.message_of_the_day()
        print("The server replied accepted our API key and sent the following greetings:")
        print("Server version:", welcome["version"])
        print("Message of the day:", welcome["message"])
    except Exception as e:
        print(f"Received error: {e} - check your API key")
        return False

    return True   
