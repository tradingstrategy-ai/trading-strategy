from capitalgram.environment.config import Configuration
from capitalgram.transport.cache import CachedHTTPTransport


def run_interactive_setup() -> Configuration:
    """Do REPL interactive setup with the user in Jupyter notebook."""

    api_key = None
    transport = CachedHTTPTransport(api_key=None)

    # Check the server is up - will throw an exception in the case of an error
    transport.ping()

    has_api_key = None
    while has_api_key not in ("y", "n"):
        print("Using Capitalgram requires an API key.")
        print("Do you have an API key yet? [y/n]")
        has_api_key = input()

    if has_api_key == "y":
        pass
    else:
        print("The Professional account and API key costs $990, but we will offer it for free for people who sign up on the mailing list during the private beta.")
        print("Would you like to sign up on the mailing list and set up an API key now? [y/n]")
        reply = input().lower()
        if reply == 'n':
            print("Thank you. See you next time.")

        print("Please give your first name:")
        first_name = input()

        print("Please give your last name:")
        last_name = input()

        print("Please give a valid email - the API key will be sent to this email address:")
        email = input()

        transport.register(email, first_name, last_name)
        print(f"Signed up on the newsletter: {email} - please check your email for the API key")

    valid_api_key = False
    while not valid_api_key:
        print("Enter your API key from the welcome email:")
        api_key = input()

        authenticated_transport = CachedHTTPTransport(api_key=api_key)
        try:
            welcome = authenticated_transport.message_of_the_day()
            print("The server replied with the message of the day:")
            print(welcome["version"])
            print(welcome["message"])
        except Exception as e:
            print(f"Received error: {e} - check your API key")
            continue

        valid_api_key = True

    config = Configuration(api_key=api_key)

    print("Setting up new API complete.")

    return config






