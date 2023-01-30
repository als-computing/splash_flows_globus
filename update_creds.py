import os

import pyotp
from myproxy.client import MyProxyClient


def update_nersc_creds(
    nersc_address,
    nersc_username,
    nersc_password,
    nersc_otp_key,
    nersc_credentials_file):

    totp = pyotp.TOTP(nersc_otp_key)
    nersc_password = nersc_password + totp.now()

    myproxy_clnt = MyProxyClient(hostname=nersc_address)
    cert, private_key = myproxy_clnt.logon(nersc_username, nersc_password, bootstrap=True)
    nersc_pem = (cert + private_key).decode()
    # breakpoint()
    print(f'Writing credentials to {nersc_credentials_file}')
    with open(nersc_credentials_file, 'w') as pem_file:
        pem_file.write(nersc_pem)

if __name__ == "__main__":
    nersc_address = os.getenv("NERSC_ADDRESS")
    nersc_username = os.getenv("NERSC_USER_NAME")
    nersc_pw = os.getenv("NERSC_PW")
    nersc_otp_key = os.getenv("NERSC_OTP_KEY")
    nersc_credentials_file = os.getenv("NERSC_CREDENTIALS_FILE")
    update_nersc_creds(nersc_address, nersc_username, nersc_pw, nersc_otp_key, nersc_credentials_file)