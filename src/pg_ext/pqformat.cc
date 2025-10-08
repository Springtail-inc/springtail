#include <cassert>

#include <pg_ext/pqformat.hh>
#include <pg_ext/string.hh>
#include <pg_repl/pg_types.hh>
#include <common/logging.hh>
#include <pg_ext/memory.hh>
#include <cstdlib>

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
    memcpy((char *)(buf->data + buf->len), &ni, sizeof(uint64_t));
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
    union {
        double f;
        int64_t i;
    } swap;

    swap.f = f;
    pq_sendint64(buf, swap.i);
}

char *
pg_client_to_server(const char *s, int len)
{
	// return pg_any_to_server(s, len, ClientEncoding->encoding);
    // XXX Stubbed for now

    return const_cast<char *>(s);
}

char *
pg_server_to_any(const char *s, int len, int encoding)
{
    // if (len <= 0) {
    //     return unconstify(char *, s); /* empty string is always valid */
    // }

    // if (encoding == DatabaseEncoding->encoding || encoding == PG_SQL_ASCII) {
    //     return unconstify(char *, s); /* assume data is valid */
    // }

    // if (DatabaseEncoding->encoding == PG_SQL_ASCII) {
    //     /* No conversion is possible, but we must validate the result */
    //     (void)pg_verify_mbstr(encoding, s, len, false);
    //     return unconstify(char *, s);
    // }

    // /* Fast path if we can use cached conversion function */
    // if (encoding == ClientEncoding->encoding) {
    //     return perform_default_encoding_conversion(s, len, false);
    // }

    // /* General case ... will not work outside transactions */
    // return (char *)pg_do_encoding_conversion((unsigned char *)unconstify(char *, s), len,
    //                                          DatabaseEncoding->encoding, encoding);

    // XXX Stubbed for now

    return const_cast<char *>(s);
}

char *
pg_server_to_client(const char *s, int len)
{
    // return pg_server_to_any(s, len, ClientEncoding->encoding);
    // XXX Stubbed for now

    return const_cast<char *>(s);
}

void
pq_sendtext(StringInfo buf, const char *str, int slen)
{
    char *p;

    p = pg_server_to_client(str, slen);
    if (p != str) /* actual conversion has been done? */
    {
        slen = strlen(p);
        appendBinaryStringInfo(buf, p, slen);
        free(p);
    } else {
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
    uint64_t n64;

    pq_copymsgbytes(msg, (char *)&n64, sizeof(n64));

    return pg_ntoh64(n64);
}

double
pq_getmsgfloat8(StringInfo msg)
{
    union {
        double f;
        uint64_t i;
    } swap;

    swap.i = pq_getmsgint64(msg);

    return swap.f;
}

bytea *
pq_endtypsend(StringInfo buf)
{
    bytea *result = (bytea *)buf->data;

    /* Insert correct length into bytea length word */
    assert(buf->len >= VARHDRSZ);
    SET_VARSIZE(result, buf->len);

    return result;
}

uint32_t
pq_getmsgint(StringInfo msg, int b)
{
    uint32_t result;
    char n8;
    uint16_t n16;
    uint32_t n32;

    switch (b) {
        case 1:
            pq_copymsgbytes(msg, (char *)&n8, 1);
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
    char *str;
    char *p;

    if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor)) {
        LOG_ERROR("Insufficient data left in message");
    }
    str = &msg->data[msg->cursor];
    msg->cursor += rawbytes;

    p = pg_client_to_server(str, rawbytes);
    if (p != str) { /* actual conversion has been done? */
        *nbytes = strlen(p);
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
