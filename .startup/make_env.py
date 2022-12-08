import argparse
import json
from pathlib import Path
import re
import subprocess
from typing import Dict, List, Optional

MAX_TRIES = 3


def get_user_uid() -> str:
    result = subprocess.check_output('echo "$(id -u)"', shell=True)
    result = subprocess.check_output('echo "$(id -u)"', shell=True, encoding="utf-8")
    result.strip()
    return result


def dot_env_file_already_exists(project_dir: Path, file_name: str = ".env") -> bool:
    project_dir = Path(project_dir)

    if any([p for p in project_dir.iterdir() if p.name == file_name]):
        print(f"A dot-env file named {file_name} already exists in this project.")
        print("To create new dot-env files via this makefile recipe, delete or move that file")
        print(f"and rerun this makefile recipe.")
        return True
    return False


def load_env_var_defaults_file(project_dir: Path) -> Dict:
    default_env_vars_file_path = Path(project_dir).joinpath(".startup", "env_var_defaults.json")
    with open(default_env_vars_file_path, "r") as jf:
        default_env_vars_json = json.load(jf)
    return default_env_vars_json


def get_and_validate_user_input(
    env_var: str,
    default_value: str,
    valid_input_pattern: Optional[str] = None,
    invalid_substrings: List[str] = [" ", "\n", "\t"],
    max_tries: int = MAX_TRIES,
) -> str:
    msg = f"{env_var} [leave blank for default value: '{default_value}']: "
    tries_remaining = max_tries
    try:
        while tries_remaining > 0:
            input_val = input(msg)
            if input_val == "":
                return default_value

            if isinstance(invalid_substrings, str) and (invalid_substrings in input_val):
                print(f"Invalid value entered, can't contain this substring: {invalid_substrings}")
                tries_remaining = tries_remaining - 1
                continue
            elif isinstance(invalid_substrings, list) and (
                any(ss in invalid_substrings for ss in input_val)
            ):
                invalid_substring_str = (
                    '"' + '", "'.join(str(iss) for iss in invalid_substrings) + '"'
                )
                print(
                    f"Invalid value entered, can't contain these substrings: {invalid_substring_str}"
                )
                tries_remaining = tries_remaining - 1
                continue
            elif valid_input_pattern is not None:
                if re.match(valid_input_pattern, input_val):
                    return input_val
                else:
                    print(f"Invalid value entered, must match pattern {valid_input_pattern}")
                    tries_remaining = tries_remaining - 1
                    continue
            return input_val
    except KeyboardInterrupt:
        print("Keyboard interrupted")


def orchestrate_user_input_prompts(env_var_dict: Dict) -> Dict:
    for env_var_id, env_var_payload in env_var_dict.items():
        if env_var_payload["user_input"] == True:
            env_var_dict[env_var_id]["set_value"] = get_and_validate_user_input(
                env_var=env_var_payload["name"],
                default_value=env_var_payload["default_value"],
                valid_input_pattern=env_var_payload["valid_pattern"],
                invalid_substrings=env_var_payload["invalid_substrings"],
            )
        elif env_var_payload["dependant_on_other_env_vars"] == True:
            env_var_mapper = env_var_payload["env_var_mappings"]
            set_value = env_var_payload["default_value"]
            for other_env_var_name, other_env_var_id in env_var_mapper.items():
                replace_to = env_var_dict[other_env_var_id]["set_value"]
                set_value = set_value.replace(f"{other_env_var_name}", replace_to)
                env_var_dict[env_var_id]["set_value"] = set_value
        elif env_var_payload["dependant_on_other_env_vars"] == False:
            env_var_dict[env_var_id]["set_value"] = env_var_payload["default_value"]
    return env_var_dict


def get_env_var_payloads(env_var_dict: Dict) -> List:
    env_var_payloads = [v for k, v in env_var_dict.items()]
    return env_var_payloads


def get_distinct_dot_env_file_names(env_var_payloads: List) -> List:
    return list(set([p["file"] for p in env_var_payloads]))


def create_dot_env_files(project_dir: Path, env_var_dict: Dict) -> None:
    env_var_payloads = get_env_var_payloads(env_var_dict=env_var_dict)
    dot_env_file_names = get_distinct_dot_env_file_names(env_var_payloads=env_var_payloads)
    for file_name in dot_env_file_names:
        file_payloads = [p for p in env_var_payloads if p["file"] == file_name]
        distinct_groups_in_file = list(set([p["group"] for p in file_payloads]))
        distinct_groups_in_file.sort()
        file_lines = []
        for group in distinct_groups_in_file:
            file_lines.append(f"# {group}")
            file_group_payloads = [p for p in file_payloads if p["group"] == group]
            for file_group_payload in file_group_payloads:
                file_lines.append(f"{file_group_payload['name']}={file_group_payload['set_value']}")
            file_lines.append("")
        file_out_path = project_dir.joinpath(file_name)
        with open(file_out_path, "x") as f:
            all_file_lines = "".join([f"{line}\n" for line in file_lines])
            all_file_lines = all_file_lines.replace("\n\n\n", "\n\n")
            all_file_lines = re.sub(r"(\n\n)$", "\n", all_file_lines)
            f.write(all_file_lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_dir", help="The project's top-level directory")
    args = parser.parse_args()

    print("Please enter Environment Variable values at the following prompts:")
    print("(you can manually edit these values in the .env and .dwh.env files later, ")
    print("just be aware that some env-var-values are made of other env-var-values)")

    project_dir = Path(args.project_dir)
    dot_env_exists = dot_env_file_already_exists(project_dir=args.project_dir, file_name=".env")
    dot_dwh_dot_env_exists = dot_env_file_already_exists(
        project_dir=args.project_dir, file_name=".dwh.env"
    )
    if (dot_env_exists == False) and (dot_dwh_dot_env_exists == False):
        env_var_dict = load_env_var_defaults_file(project_dir=args.project_dir)
        env_var_dict = orchestrate_user_input_prompts(env_var_dict=env_var_dict)
        env_var_dict[".env::AIRFLOW_UID"] = {
            "file": ".env",
            "name": "AIRFLOW_UID",
            "group": "Airflow",
            "set_value": get_user_uid(),
        }
        create_dot_env_files(project_dir=project_dir, env_var_dict=env_var_dict)
