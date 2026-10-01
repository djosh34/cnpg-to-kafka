#!/usr/bin/env bash
# Disposable public test identities only. Never copy production keys here.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
certs="$root/integration/.certs"
mkdir -p "$certs"
chmod 700 "$certs"
umask 077
cd "$certs"
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 7 \
  -keyout ca.key -out ca.crt -subj '/CN=cnpg-e2e-ca' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl req -new -newkey rsa:2048 -nodes -keyout server.key \
  -out server.csr -subj '/CN=localhost'
printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=serverAuth' \
  'subjectAltName=DNS:localhost,DNS:redpanda,IP:127.0.0.1' > server.ext
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 7 -sha256 -extfile server.ext
openssl req -new -newkey rsa:2048 -nodes -keyout client.key \
  -out client.csr -subj '/CN=cnpg-e2e-client'
printf '%s\n' 'basicConstraints=critical,CA:FALSE' \
  'keyUsage=critical,digitalSignature,keyEncipherment' \
  'extendedKeyUsage=clientAuth' > client.ext
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 7 -sha256 -extfile client.ext
# A different CA and identity for explicit trust-rejection tests.
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 7 \
  -keyout wrong-ca.key -out wrong-ca.crt -subj '/CN=cnpg-e2e-untrusted-ca' \
  -addext 'basicConstraints=critical,CA:TRUE' \
  -addext 'keyUsage=critical,keyCertSign,cRLSign'
openssl req -new -newkey rsa:2048 -nodes -keyout wrong-client.key \
  -out wrong-client.csr -subj '/CN=cnpg-e2e-untrusted-client'
openssl x509 -req -in wrong-client.csr -CA wrong-ca.crt -CAkey wrong-ca.key \
  -CAcreateserial -out wrong-client.crt -days 7 -sha256 -extfile client.ext
rm -f ./*.csr ./*.ext ./*.srl ca.key wrong-ca.key
