#########################
######## Warning ########
#########################

This is a work in progress - this warning will remain
here until the deploy script can run a full deployment
and provide a functioning Leonardo environment.

 ISSUES TO RESOLVE:
   * Authentication fails when user's access notebook via the Leonardo-proxy.
   * Running without SAM server (get rid of extraneous warnings or disable entirely).


# What is this?

deploy.py is a vanilla Python 2.7 script that will build
Leonardo, push images to GCR, and deploy a leonardo server to
the a Google Compute Engine VM.


# Quickstart

The simplest deploy possible is done by running deploy.py
while specifying '--project', '--region', and '--host=IP',
and '--oauth2-client-id' flags. The default configuration
in leonardo.conf.template will work in many cases but, at
minimum, you should add your email to the auth providerConfig
whitelist or replace auth.providerConfig entirely.

Read onward for details on how to:
  * Configure DNS
  * Configure an OAuth 2.0 client
  * Reuse an existing SQL server
  * Encrypt and send SSL certificates


# Details

## Speeding things up:
 * Cloud SQL:
    - If you have an existing Cloud SQL server. Specify the flag
      '--use-existing-sql' and provide the '--cloud-sql-name'.
      argument.
 * Skipping build:
    - If you've already built and pushed your docker images, you
      can skip this step by passing "--nobuild" as a bare flag.


## OAuth 2.0 Client ID

Visit the the Cloud Console's APIs and Services credential page
(linked below) and create an OAuth 2.0 client ID. During creation
you will need to configure the callback url as https://YOUR-HOST/o2c.html
 * https://console.cloud.google.com/apis/credentials


## DNS configuration and '--host'

The deploy script can detect existing unused static IPs and
will allow assignment, alternatively it will demand that a
static IP is allocated. It is up to the user to ensure that
the DNS record is created for this IP. You can pass the flag
'--host=IP' to force the use of a machines IP address as the
host.


## SSL Certificates:

The default behavior of the deploy script is to generate
self-signed certificate for all Leonardo components. In some cases
it is necessary to provide certificates externally. This is especially
important when:

  * Certificates for the front-end are signed by a certificate authority.
  * For an existing Leonardo deploy, generating new back-end certificates
    will cause the connection with existing notebook servers to fail.


### Sending SSL Certificates to Leo

#### Quickstart

Use cloud kms to encrypt certificate files, store them in GCS, and
set appropriate flags allowing the Leonardo startup script to find
and decrypt the files. Detailed instuctions are included below.


#### List of SSL Flags

  --kms-project:   project of kms keyring (defaults to --project)
  --kms-location:  location of kms keyring (defaults to global)
  --kms-keyring:   name of kms keyring
  --kms-key:       name of kms key
  --rootca-key:    GCS path of the encrypted root CA key file used for
                   secure communication between notebooks and Leonardo.
  --ssl-cert:      GCS path of the encrypted front-end certificate
  --ssl-key:       GCS path of the encrypted front-end key
  --ssl-ca-bundle  GCS path of the encrypted front-end CA bundle


#### Recipe for sending SSL certs

  1) Generate or procure a root certificate authority key for internal
     use. You can generate a key using the following openssl command.
       $ openssl genrsa -out $KEYROOT/rootCA.key 2048 -des3

  2) Generate or procure front-end signed certificates.

  3) Next you will need to generate a keyring and encryption key with
     Google Cloud KMS. See full instructions in the link below.
       * https://cloud.google.com/kms/docs/quickstart#encrypt_data

  4) Encrypt your data using the gcloud tool as below and copy the
     encrypted data to a GCS bucket.
       $ gcloud kms encrypt \
           --plaintext-file rootCA.key \
           --ciphertext-file rootCA.key.ciphertext \
           --keyring=KEYRING-NAME \
           --key=KEY-NAME \
           --location=global
       $ gsutil cp rootCA.key.ciphertext gs://MY-BUCKET/ssl/rootCA.key.ciphertext

  5) Now you can use flags in deploy.py to consume the encrypted keys
     during VM initialization.
       $ deploy.py <other-options> \
           --kms-location=global \
           --kms-keyring=KEYRING-NAME \
           --kms-key=KEY-NAME \
           --rootca-key=gs://MY-BUCKET/ssl/rootCA.key.ciphertext