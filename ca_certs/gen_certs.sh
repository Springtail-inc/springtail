#!/bin/bash

rm -f *.csr *.pem *.srl

# Generate a self-signed root CA key and certificate
openssl genrsa -out ca_key.pem 2048
openssl req -x509 -new -nodes -key ca_key.pem -sha256 -days 365 -out ca_cert.pem \
    -subj "/C=US/ST=California/L=San Francisco/O=Springtail/CN=springtail_grpc"

# Generate a private key for the server
openssl genrsa -out server_key.pem 2048

# Create a certificate signing request (CSR) for the server using a config file
openssl req -new -key server_key.pem -out server.csr -config openssl.cnf \
    -subj "/C=US/ST=California/L=San Francisco/O=Springtail/CN=springtail_server"

# Sign the server certificate using the CA and apply extensions from the config file
openssl x509 -req -in server.csr -CA ca_cert.pem -CAkey ca_key.pem -CAcreateserial \
    -out server_cert.pem -days 365 -extfile openssl.cnf -extensions v3_req

# Generate a private key for the client
openssl genrsa -out client_key.pem 2048

# Create a certificate signing request (CSR) for the client using a config file
openssl req -new -key client_key.pem -out client.csr -config client_openssl.cnf \
    -subj "/C=US/ST=California/L=San Francisco/O=Springtail/CN=springtail_client"

# Sign the client certificate using the CA and apply extensions from the config file
openssl x509 -req -in client.csr -CA ca_cert.pem -CAkey ca_key.pem -CAcreateserial \
    -out client_cert.pem -days 365 -extfile client_openssl.cnf -extensions v3_req
