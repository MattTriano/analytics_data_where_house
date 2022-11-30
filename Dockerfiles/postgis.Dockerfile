FROM postgis/postgis:15-3.3
RUN apt-get update \
      && apt-get install -y --no-install-recommends \
           jq \
      && apt-get autoremove -yqq --purge \
      && apt-get clean \
      && rm -rf /var/lib/apt/lists/*