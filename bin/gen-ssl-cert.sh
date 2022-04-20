#!/usr/bin/env bash

# This script and the certs/openssl.cnf were determined from this page
# https://www.golinuxcloud.com/openssl-generate-ecc-certificate/
#
# The important bit is the `-extensions v3_ca` which requires a custom
# openssl.cnf that enables the v3_ca extension.
#
# Without this, the rustls client would always fail with the following:
#     thread 'test_client_server_basic' panicked at 'error connecting: Custom { kind: InvalidData, error: InvalidCertificateData("invalid peer certificate: UnsupportedCertVersion") }
#
# The documentation of this error, here: https://docs.rs/x509-signature/latest/x509_signature/enum.Error.html
# says `UnsupportedCertVersion: Version is not 3`
# Meaning, the default openssl config generates some other cert version that
# the rustls client's `with_safe_defaults` deems "unsafe"


set -ex

openssl genrsa -out certs/key.pem 4096
openssl rsa -in certs/key.pem -pubout -out certs/public-key.pem
openssl req -new -x509 -key certs/key.pem -out certs/cert.pem -days 3650 -extensions v3_ca -config certs/openssl.cnf
