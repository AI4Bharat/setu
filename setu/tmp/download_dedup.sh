#!/bin/usr/env bash

lang="hindi"

gsutil -m cp -r gs://sangraha/dedup/minhash/$lang /data/priyam/sangraha/dedup/minhash

# gsutil -m cp -r gs://sangraha/dedup/minhash/parquets/$lang /data/priyam/sangraha/dedup/minhash/parquets