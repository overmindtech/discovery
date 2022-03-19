#!/bin/sh
set -e

echo "Deleting all old NSC content..."
rm -rf /nsc/config /nsc/data /nsc/keys

echo "Generating new keys..."
nats-token-exchange init --nsc-storage /nsc --nsc-operator overmind-testing

echo "Done!"
