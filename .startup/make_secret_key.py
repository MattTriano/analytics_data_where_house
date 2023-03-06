import subprocess


def generate_secret_key_for_flask() -> str:
    output = subprocess.run(["openssl", "rand", "-base64", "42"], capture_output=True)
    if output.returncode != 0:
        raise Exception(f"SECRET_KEY generating func returned nonzero code {output.returncode}")
    secret_key = output.stdout.decode().strip()
    return secret_key


if __name__ == "__main__":
    if "__file__" in globals():
        print(generate_secret_key_for_flask())
