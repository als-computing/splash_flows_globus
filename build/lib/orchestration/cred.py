import os
from pysqlcipher3 import dbapi2 as sqlite
import peewee
from playhouse.sqlcipher_ext import SqlCipherDatabase, TextField

# auth_db = "./password.db"
db = None
db_proxy = peewee.Proxy()


class Model(peewee.Model):
    class Meta:
        database = db_proxy


class Triggers(Model):
    name = TextField(primary_key=True)
    url = TextField()


class Credentials(Model):
    name = TextField(primary_key=True)
    target = TextField()
    root_path = TextField()
    uri = TextField()
    uri_text = TextField()
    auth_mode = TextField()
    username = TextField()
    password = TextField()
    otp = TextField()


def initialize(passphrase, path):
    db = SqlCipherDatabase(path, passphrase)
    db_proxy.initialize(db)


def get_credentials(key):
    result = Credentials.select()

    for res in result:
        if res.name != key:
            continue
        return res
    return None


def get_trigger(key):
    result = Triggers.select()

    for res in result:
        if res.name != key:
            continue
        return res
    return None
