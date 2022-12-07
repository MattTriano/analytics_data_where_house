import argparse
import json
from pathlib import Path
import re
import subprocess
from typing import Dict, List, Optional

MAX_TRIES = 3

# RELAXED_EMAIL_PATTERN = re.compile(r"[^@\s]+@[^@\s]+\.[\w]+$")
# WHITESPACE_PATTERN = re.compile(r"[\s]")

def get_user_uid() -> str:
    result = subprocess.check_output('echo "$(id -u)"', shell=True)
    result = subprocess.check_output(
        'echo "$(id -u)"', shell=True, encoding="utf-8"
    )
    result.strip()
    return result


def dot_env_file_already_exists(project_dir: Path, file_name: str = ".env") -> bool:
    print(f"project_dir: {project_dir}, {type(project_dir)}")
    project_dir = Path(project_dir)
    print(f"project_dir: {project_dir}, {type(project_dir)}")

    if any([p for p in project_dir.iterdir() if p.name == file_name]):
        print(f"A dot-env file named {file_name} already exists in this project.")
        print("To create new dot-env files via this makefile recipe, delete or move that file")
        print(f"and rerun this makefile recipe.")
        return True
    return False


def load_env_var_defaults_file(project_dir: Path) -> Dict:
    dafault_env_vars_file_path = Path(project_dir).joinpath(".startup", "env_var_defaults.json")
    with open(dafault_env_vars_file_path, "r") as jf:
        default_env_vars_json = json.load(jf)
    return default_env_vars_json

def get_and_validate_user_input(
    env_var: str, default_value: str, valid_input_pattern: Optional[str] = None,
    invalid_substrings: List[str] = [" ", "\n", "\t"], max_tries: int = MAX_TRIES
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
            elif isinstance(invalid_substrings, list) and (any(ss in invalid_substrings for ss in input_val)):
                invalid_substring_str = (
                    '"' + '", "'.join(str(iss) for iss in invalid_substrings) + '"'
                )
                print(f"Invalid value entered, can't contain these substrings: {invalid_substring_str}")
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
        print(f"env_var_id: {env_var_id}")
        # print(f"env_var_payload: {env_var_payload}")
        if env_var_payload["user_input"] == True:
            env_var_dict[env_var_id]["set_value"] = get_and_validate_user_input(
                env_var=env_var_payload["name"],
                default_value=env_var_payload["default_value"],
                # input_val=env_var_payload["default_value"],
                valid_input_pattern=env_var_payload["valid_pattern"],
                invalid_substrings=env_var_payload["invalid_substrings"],
            )
        elif env_var_payload["dependant_on_other_env_vars"] == True:
            env_var_mapper = env_var_payload["env_var_mappings"]
            set_value = env_var_payload["default_value"]
            print(env_var_mapper)
            for other_env_var_name, other_env_var_id in env_var_mapper.items():
                replace_to = env_var_dict[other_env_var_id]["set_value"]
                print(f'Replacing {other_env_var_name} with {replace_to} in {set_value}')
                set_value = set_value.replace(f"{other_env_var_name}", replace_to)
                print(f"Now set_value is {set_value}")
                env_var_dict[env_var_id]["set_value"] = set_value
        elif env_var_payload["dependant_on_other_env_vars"] == False:
            env_var_dict[env_var_id]["set_value"] = env_var_payload["default_value"]
    return env_var_dict

# def flatten_file_env_var_dict(env_var_dict):
#     outer = list(env_var_dict.values())
#     return [item for inner in outer for item in inner]

# def orchestrate_user_input_prompts(default_env_vars_json: Dict) -> Dict:
#     # This function stinks to high heaven and if I have to change env_vars much, I'll probably
#     #  really want to pick a better data structure, or rethink this solution to shielding users
#     #  from having to formulate connection strings/enter the same env_var values in different
#     #  places.
#     lines_for_all_env_files = {}
#     for file_name in default_env_vars_json.keys():
#         cred_groups = default_env_vars_json[file_name]
#         env_file_lines = []
#         for cred_group in cred_groups.keys():
#             env_file_lines.append(f"# {cred_group}")
#             cred_group_env_vars = cred_groups[cred_group]
#             set_env_vars = {}
#             if "user_input_vars" in cred_group_env_vars.keys():
#                 user_input_env_vars = cred_group_env_vars["user_input_vars"]                
#                 for env_var_name, default_env_var in user_input_env_vars.items():
#                     # TODO replace with actual input func
#                     print(f"        Input Prompt: {env_var_name} [{default_env_var}]: ")
#                     get_and_validate_user_input(
#                         env_var=env_var_name,
#                     )
#                     user_input = default_env_var
#                     set_env_vars[env_var_name] = user_input
#                     env_file_lines.append(f"{env_var_name}={user_input}")
#             if "other_vars" in cred_group_env_vars.keys():
#                 other_env_vars = cred_group_env_vars["other_vars"]
#                 for other_env_var, default_value in other_env_vars.items():
#                     set_env_var_value = default_value
#                     set_env_var_lines = flatten_file_env_var_dict(lines_for_all_env_files)
#                     if any(default_value in line for line in set_env_var_lines):
#                         matching_vals = ["=".join(el.split("=")[1:]) for el in set_env_var_lines if default_value in el]
#                         set_env_var_value = matching_vals[0]
#                     elif isinstance(default_value, str):
#                         existing_env_vars = re.findall("(?<=<)([\S]*?)(?=>)", default_value)
#                         if len(existing_env_vars) > 0:
#                             for existing_env_var in existing_env_vars:
#                                 existing_env_var_value = set_env_vars[existing_env_var]
#                                 set_env_var_value = set_env_var_value.replace(f"<{existing_env_var}>", existing_env_var_value)
#                     env_file_lines.append(f"{other_env_var}={set_env_var_value}")
#             env_file_lines.append("")
#         lines_for_all_env_files[file_name] = env_file_lines
#     return lines_for_all_env_files

def get_env_var_payloads(env_var_dict: Dict) -> List:
    env_var_payloads = [v for k, v in env_var_dict.items()]
    return env_var_payloads

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_dir', help="The project's top-level directory")
    args = parser.parse_args()

    user_uid = get_user_uid()
    print(f"User uid bit: AIRFLOW_UID={user_uid}")
    print("Please enter Environment Variable values at the following prompts:")
    print("(you can manually edit these values in the .env and .dwh.env files later, ")
    print("just be aware that some env-var-values are made of other env-var-values)")

    project_dir = Path(args.project_dir)
    print(f"project_dir: {project_dir}")
    dot_env_exists = dot_env_file_already_exists(project_dir=args.project_dir, file_name=".env")
    dot_dwh_dot_env_exists = dot_env_file_already_exists(project_dir=args.project_dir, file_name=".dwh.env")
    if (dot_env_exists == False) and (dot_dwh_dot_env_exists == False):
        env_var_dict = load_env_var_defaults_file(project_dir=args.project_dir)
        env_var_dict = orchestrate_user_input_prompts(env_var_dict=env_var_dict)
        env_var_payloads = get_env_var_payloads(env_var_dict=env_var_dict)
        # print(env_var_dict)
