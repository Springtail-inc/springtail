#include <cassert>

#include <pg_ext/pqformat.hh>
#include <pg_ext/string.hh>
#include <pg_repl/pg_types.hh>
#include <common/logging.hh>
#include <pg_ext/memory.hh>
#include <cstdlib>
#include <cstring>

#include <arpa/inet.h>

static inline uint16_t
pg_ntoh16(uint16_t x)
{
    return ntohs(x);
}

static inline uint32_t
pg_ntoh32(uint32_t x)
{
    return ntohl(x);
}

static inline uint64_t
pg_ntoh64(uint64_t x)
{
    return be64toh(x);
}

void
pq_writeint64(StringInfo buf, uint64_t i)
{
    uint64_t ni = springtail::pg_hton64(i);

    assert(buf->len + (int)sizeof(uint64_t) <= buf->maxlen);
    memcpy(buf->data + buf->len, &ni, sizeof(uint64_t));
    buf->len += sizeof(uint64_t);
}

void
pq_sendint64(StringInfo buf, uint64_t i)
{
    enlargeStringInfo(buf, sizeof(uint64_t));
    pq_writeint64(buf, i);
}

void
pq_sendfloat8(StringInfo buf, double f)
{
    int64_t bits = std::bit_cast<int64_t>(f);
    pq_sendint64(buf, bits);
}

char *
pg_client_to_server(const char *s, int len = 0)
{
    // XXX Stubbed for now
    return nullptr;
}

char *
pg_server_to_any(const char *s, int len = 0, int encoding = 0)
{
    // XXX Stubbed for now
    return nullptr;
}

char *
pg_server_to_client(const char *s, int len = 0)
{
    // XXX Stubbed for now
    return nullptr;
}

void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
    char *p;

    if (!str || slen <= 0) {
        LOG_ERROR("Invalid arguments to pq_sendtext");
        return;
    }

    p = pg_server_to_client(str, slen);
    if (p && p != str) /* actual conversion has been done? */
    {
        // Converted string is NUL-terminated by converter; still, be defensive
        int plen = (int)strnlen(p, slen); /* cap to original length */
        appendBinaryStringInfo(buf, p, plen);
        pfree(p);
    }
    else {
        appendBinaryStringInfo(buf, str, slen);
    }
}

void
pq_copymsgbytes(StringInfo msg, char *buf, int datalen)
{
    if (datalen < 0 || datalen > (msg->len - msg->cursor)) {
        LOG_ERROR("Insufficient data left in message");
    }

    memcpy(buf, &msg->data[msg->cursor], datalen);
    msg->cursor += datalen;
}

int64_t
pq_getmsgint64(StringInfo msg)
{
    uint64_t n64 = 0;

    pq_copymsgbytes(msg, (char *)&n64, sizeof(n64));

    return pg_ntoh64(n64);
}

double
pq_getmsgfloat8(StringInfo msg)
{
    uint64_t bits = pq_getmsgint64(msg);
    double f = std::bit_cast<double>(bits);
    return f;
}

bytea *
pq_endtypsend(StringInfo buf)
{
    auto *result = (bytea *)buf->data;

    /* Insert correct length into bytea length word */
    assert(buf->len >= VARHDRSZ);
    SET_VARSIZE(result, buf->len);

    return result;
}

uint32_t
pq_getmsgint(StringInfo msg, int b)
{
    auto result = 0u;
    char n8 = 0;
    uint16_t n16 = 0;
    uint32_t n32 = 0;

    switch (b) {
        case 1:
            pq_copymsgbytes(msg, &n8, 1);
            result = n8;
            break;
        case 2:
            pq_copymsgbytes(msg, (char *)&n16, 2);
            result = pg_ntoh16(n16);
            break;
        case 4:
            pq_copymsgbytes(msg, (char *)&n32, 4);
            result = pg_ntoh32(n32);
            break;
        default:
            LOG_WARN("Unsupported integer size {}", b);
            result = 0;
            break;
    }
    return result;
}

char *
pq_getmsgtext(StringInfo msg, int rawbytes, int *nbytes)
{
    const char *str = nullptr;
    char *p = nullptr;

    if (!msg || !nbytes) {
        LOG_ERROR("Invalid arguments to pq_getmsgtext");
        return nullptr;
    }
    if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor)) {
        LOG_ERROR("Insufficient data left in message");
        return nullptr;
    }
    str = &msg->data[msg->cursor];
    msg->cursor += rawbytes;

    p = pg_client_to_server(str, rawbytes);
    if (p && p != str) { /* actual conversion has been done? */
        *nbytes = (int)strnlen(p, rawbytes); /* do not read past provided size */
    } else {
        p = (char *)palloc(rawbytes + 1);
        memcpy(p, str, rawbytes);
        p[rawbytes] = '\0';
        *nbytes = rawbytes;
    }
    return p;
}

void
pq_begintypsend(StringInfo buf)
{
	initStringInfo(buf);
	/* Reserve four bytes for the bytea length word */
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
	appendStringInfoCharMacro(buf, '\0');
}
