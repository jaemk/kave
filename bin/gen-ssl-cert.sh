#!/usr/bin/env bash

set -ex

openssl genrsa -out certs/key.pem 4096
openssl rsa -in certs/key.pem -pubout -out certs/public-key.pem
openssl req -new -x509 -key certs/key.pem -out certs/cert.pem -days 3650

