from cryptography.fernet import Fernet


def generate_fernet_key_value() -> str:
    fernet_key = Fernet.generate_key()
    return fernet_key.decode()  # the fernet_key value you'd put in a dot-env (.env) file


if __name__ == "__main__":
    if "__file__" in globals():
        print(generate_fernet_key_value())
