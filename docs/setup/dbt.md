### Specifying, installing, and updating dbt packages
 
Create a file named `packages.yml` in your dbt project directory and specify any packages you want to use in your project in the format shown below (or as shown in the [documentation](https://docs.getdbt.com/docs/build/packages))

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 0.9.2
```

Then, after specifying packages and versions to use, run this command to install packages.

```bash
user@host:.../your_local_repo$ make update_dbt_packages
01:33:04  Running with dbt=1.3.0
01:33:05  Installing dbt-labs/dbt_utils
01:33:05    Installed from version 0.9.2
01:33:05    Up to date!
```
