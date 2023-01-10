## System Setup

Preprequisites:
To use this system, Docker is the only absolutely necessary prerequisite.

Having `GNU make` and/or core python on your host system will enable you to use included `makefile` recipes and scripts to streamline setup and common operations, but you could get by without them (although you'll have to figure more out).

### Setting up credentials
After cloning this repo and `cd`ing into your local, run this `make` command and respond to prompts the the requested values,

```bash
make make_credentials
```

#### Generating a Frenet Key to use as env var AIRFLOW__CORE__FERNET_KEY
To get a proper frenet key for the `AIRFLOW__CORE__FERNET_KEY` environment variable, the best way I know of involves the `cryptography` module, which isn't a built-in python module, but it is pretty common and it's easy enough to `pip install` or `conda install` into a `venv` or `conda env` if it hasn't already been installed as a dependency for something else.

```python
from cryptography.fernet import Fernet

fernet_key = Fernet.generate_key()
print(fernet_key.decode()) # your fernet_key
```
then copy that value and paste it into the appropriate field in the `.env` file in the same directory as this README.md file.