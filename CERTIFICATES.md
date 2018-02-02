
# Secure, 2-way communication between leonardo and jupyter
Each section shows commands to generate the necessary keys and certificates for secure 2-way 
SSL communication between leonardo and jupyter. You can use different filenames, but you 
will need to update the leonardo.conf file with the names you use.

###Generate a root CA. 
This creates a rootCA.key and a rootCA.pem.
```
openssl genrsa -out rootCA.key 2048 -des3
openssl req -x509 -new -nodes -key rootCA.key -days 1024 -out rootCA.pem -sha256 
```

###Generate the jupyter server key and certificate, signed by the CA
This creates a jupyter-server.key and a jupyter-server.crt. The domain name you use in creating the key request should be specified in the leonardo.conf in place of JUPYTER\_DOMAIN\_NAME
```
openssl genrsa -out jupyter-server.key 2048
openssl req -new -key jupyter-server.key -out jupyter-server.csr -sha256
openssl x509 -req -in jupyter-server.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out jupyter-server.crt -days 1500
```

###Generate a Leo client key and certificate, signed by the CA. 
This generates a leo-client.key and a leo-client.crt. The common name should be different than the server cert.
```
openssl genrsa -out leo-client.key 2048
openssl req -new -key leo-client.key -out leo-client.csr -sha256
openssl x509 -req -in leo-client.csr -CA rootCA.pem -CAkey rootCA.key -CAcreateserial -out leo-client.crt -days 1500
```

###Generate the keystore (PKCS12 file). 
This is used to send the client certificate to the server
```
openssl pkcs12 -export -inkey leo-client.key -in leo-client.crt -out leo-client.p12
```

