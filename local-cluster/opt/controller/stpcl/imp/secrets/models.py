from dataclasses import dataclass
from enum import Enum


class PostgresUserRole(str, Enum):
    REPLICATION = "replication"
    DATABASE = "database"
    FDW_SUPERUSER = "fdw_superuser"
    PROXY_TO_FDW = "proxy_to_fdw"


class PostgresCredType(str, Enum):
    TEXT = "text"
    MD5 = "md5"
    SCRAM_SHA_256 = "scram-sha-256"


# [{"username":"replicator","password":"R3plic@t0r","type":"text","role":"replication"},{"username":"leon","password":"rpd-rookie","type":"text","role":"database"},{"username":"_springtail_fdw_pcDkTHI-","password":"UtpcJjUGuSOzNSAyEM2CE0sh","type":"text","role":"fdw_superuser"},{"username":"-","password":"qhRe_XI8pZAFP88Q3VnT3Zhe","type":"text","role":"proxy_to_fdw"}]
@dataclass
class PostgresCred:
    username: str
    password: str
    type: str
    role: PostgresUserRole


PostgresCredentialList = list[PostgresCred]
