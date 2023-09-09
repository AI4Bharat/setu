#!/bin/usr/env bash

lang="hindi"

gsutil -m cp -r /data/priyam/sangraha/dedup/minhash/parquets/$lang gs://sangraha/dedup/minhash/parquets

rm -rf /data/priyam/sangraha/dedup/minhash/$lang
rm -rf /data/priyam/sangraha/dedup/minhash/parquets/$lang