#!/bin/bash

# The script will create the following files:
#   ca.key: Root CA private key
#   ca.crt: Root CA certificate
#   server.key: Server private key
#   server.crt: Server certificate
#   client.key: Client private key
#   client.crt: Client certificate

# Note: This script is intended for demonstration and development purposes. 
#     In a production environment, you'd want to secure the CA private key (ca.key) 
#     very carefully and potentially include additional features such as certificate 
#     revocation lists (CRLs). Furthermore, you might want to adjust the validity 
#     period (specified by -days 365) and other parameters to fit your specific requirements.



# Certificate details
COUNTRY="CA"
STATE="ON"
LOCATION="TORONTO"
ORGANIZATION="MyOrganization"
ORG_UNIT="MyOrgUnit"
EMAIL="email@example.com"

# Check for provided client details
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <ClientName> <ClientOrgUnit> <ClientEmail>"
    exit 1
fi

CLIENT_NAME="$1"
CLIENT_ORG_UNIT="$2"
CLIENT_EMAIL="$3"

# Generate root CA
openssl genpkey -algorithm RSA -out ca.key
openssl req -new -x509 -key ca.key -out ca.crt -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$ORGANIZATION/OU=$ORG_UNIT/CN=CA/emailAddress=$EMAIL" -days 365

# Generate server key and certificate request
openssl genpkey -algorithm RSA -out server.key
openssl req -new -key server.key -out server.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$ORGANIZATION/OU=$ORG_UNIT/CN=server/emailAddress=$EMAIL"

# Sign the server certificate with the root CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

# Generate client key and certificate request
openssl genpkey -algorithm RSA -out client.key
openssl req -new -key client.key -out client.csr -subj "/C=$COUNTRY/ST=$STATE/L=$LOCATION/O=$ORGANIZATION/OU=$CLIENT_ORG_UNIT/CN=$CLIENT_NAME/emailAddress=$CLIENT_EMAIL"

# Sign the client certificate with the root CA
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365

# Cleanup
rm *.csr
rm *.srl

echo "Certificates generated successfully!"
