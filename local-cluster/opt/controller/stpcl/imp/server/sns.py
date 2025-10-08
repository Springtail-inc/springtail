# Description: AWS SNS HTTP(S) endpoint with signature verification. This is a subscriber to SNS topics.
import base64
import requests
import threading
import orjson as json
from urllib.parse import urlparse
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from fastapi import (
    HTTPException,
    Request,
    APIRouter,
)

router = APIRouter()


def build_string_to_sign(m: dict) -> str:
    t = m["Type"]
    if t in ("SubscriptionConfirmation", "UnsubscribeConfirmation"):
        return (
            f"Message\n{m['Message']}\n"
            f"MessageId\n{m['MessageId']}\n"
            f"SubscribeURL\n{m['SubscribeURL']}\n"
            f"Timestamp\n{m['Timestamp']}\n"
            f"Token\n{m['Token']}\n"
            f"TopicArn\n{m['TopicArn']}\n"
            f"Type\n{m['Type']}\n"
        )
    elif t == "Notification":
        s = f"Message\n{m['Message']}\nMessageId\n{m['MessageId']}\n"
        if "Subject" in m and m["Subject"]:
            s += f"Subject\n{m['Subject']}\n"
            s += (
                f"Timestamp\n{m['Timestamp']}\n"
                f"TopicArn\n{m['TopicArn']}\n"
                f"Type\n{m['Type']}\n"
            )
        return s
    else:
        raise ValueError("unexpected Type")


_cert_cache = {}
_cert_cache_lock = threading.Lock()


def verify_signature(m: dict):
    """
    Verify the SNS message signature. Required for the HTTP endpoint to be a valid subscriber.
    :param m:
    :return:
    """
    if m.get("SignatureVersion") != "1":
        raise HTTPException(400, "unsupported SignatureVersion")
    u = urlparse(m["SigningCertURL"])
    if u.scheme != "https" or not u.netloc.endswith("amazonaws.com"):
        raise HTTPException(400, "invalid cert host/scheme")

    cert_url = m["SigningCertURL"]
    with _cert_cache_lock:
        cert = _cert_cache.get(cert_url)
        if cert is None:
            pem = requests.get(cert_url, timeout=5).content
            cert = x509.load_pem_x509_certificate(pem)
            _cert_cache[cert_url] = cert

    pub = cert.public_key()
    to_sign = build_string_to_sign(m).encode("utf-8")
    sig = base64.b64decode(m["Signature"])

    pub.verify(
        sig, to_sign,
        padding.PKCS1v15(),
        hashes.SHA1()
    )


@router.post("/notify")
async def sns(request: Request):
    """
    AWS SNS HTTP(S) endpoint with signature verification. This is a subscriber to SNS topics.
    :param request:
    :return:
    """
    sns_client = request.app.state.registry.sns_client
    body = await request.body()
    try:
        m = json.loads(body)
    except Exception:
        raise HTTPException(400, "bad json")

    # Optional header check
    hdr = request.headers.get("x-amz-sns-message-type")
    if hdr and hdr != m.get("Type"):
        raise HTTPException(400, "type mismatch")

    # Verify signature
    try:
        verify_signature(m)
    except Exception as e:
        raise HTTPException(403, f"signature invalid: {e}")

    typ = m["Type"]
    if typ == "SubscriptionConfirmation":
        try:
            sns_client.confirm_subscription(
                TopicArn=m["TopicArn"],
                Token=m["Token"],
            )
        except Exception as e:
            # Already confirmed or transient issue — still return 200
            print("ConfirmSubscription error:", e)
        return {"ok": True}

    elif typ == "Notification":
        # TODO: Process m["Message"] here in the future.
        # We should be streaming the messages to some in-memory or simple storage queue
        # Then the web UI can fetch from that queue to display the messages
        return {"ok": True}

    elif typ == "UnsubscribeConfirmation":
        return {"ok": True}

    else:
        raise HTTPException(400, "unsupported type")
