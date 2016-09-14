/*
 * (C) 2010-2011 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "tsar.h"

#define STATS_TEST_SIZE (sizeof(struct stats_redis))

static const char *redis_usage = "    --redis               redis information";

/*
 * temp structure for collection information.
 */
struct stats_redis {
    unsigned long long keys;             /* keys in db */
};

/* Structure for tsar */
static struct mod_info redis_info[] = {
        {"keys", SUMMARY_BIT, 0, STATS_NULL},
};

static void
read_redis_stats(struct module *mod, const char *parameter) {
    int write_flag = 0, addr_len, domain;
    int m, sockfd, send, pos;
    void *addr;
    char buf[LEN_4096], request[LEN_4096], line[LEN_4096];
    FILE *stream = NULL;

    struct sockaddr_in servaddr;

    char *host = "127.0.0.1";
    int port = 6379;

    /* parameter actually equals to mod->parameter */
    struct stats_redis st_redis;

    memset(buf, 0, sizeof(buf));
    memset(&st_redis, 0, sizeof(struct stats_redis));

    addr = &servaddr;
    addr_len = sizeof(servaddr);
    bzero(addr, addr_len);
    domain = AF_INET;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    inet_pton(AF_INET, host, &servaddr.sin_addr);

    if ((sockfd = socket(domain, SOCK_STREAM, 0)) == -1) {
        goto writebuf;
    }

    if ((m = connect(sockfd, (struct sockaddr *) addr, addr_len)) == -1) {
        goto writebuf;
    }

    sprintf(request,
            "%s", "*2\r\n$4\r\ninfo\r\n$3\r\nall\r\n");

    if ((send = write(sockfd, request, strlen(request))) == -1) {
        goto writebuf;
    }

    if ((stream = fdopen(sockfd, "r")) == NULL) {
        goto writebuf;
    }

    char cur_byte = fgetc(stream);
    if ('$' != cur_byte || EOF == cur_byte) {
        goto writebuf;
    }

    // get the length of bulk reply
    int length = 0;
    while ((cur_byte = fgetc(stream)) != '\r') {
        if (EOF == cur_byte) {
            goto writebuf;
        }
        length *= 10;
        length += cur_byte - '0';
    }

    // skip '\n'
    if (EOF == fgetc(stream)) {
        goto writebuf;
    }

    unsigned long long keys = 0, cur_keys = 0, db, expires, avg_ttl;
    bool keyspace_field = false;

    // read all content of bulk replay
    // including the last CRLF
    int read_len = 0;
    while (fgets(line, LEN_4096, stream) != NULL) {
        if (!strncmp(line, "# Keyspace", sizeof("# Keyspace") - 1)) {
            keyspace_field = true;
        } else if (keyspace_field) {
            if (!strncmp(line, "db", sizeof("db") - 1)) {
                // db0:keys=3,expires=0,avg_ttl=0
                sscanf(line, "db%llu:keys=%llu,expires=%llu,avg_ttl=%llu",
                       &db, &cur_keys, &expires, &avg_ttl);
                keys += cur_keys;
                write_flag = 1;
            }
        }



        // update read len
        read_len += strlen(line);
        if (read_len == 2 + length) {
            break;
        }
    }

    st_redis.keys = keys;

    writebuf:
    if (stream) {
        fclose(stream);
    }

    if (sockfd != -1) {
        close(sockfd);
    }

    pos = sprintf(buf, "%llu",
    /* the store order is not same as read procedure */
                  st_redis.keys);

    buf[pos] = '\0';
    /* send data to tsar you can get it by pre_array&cur_array at set_redis_record */
    set_mod_record(mod, buf);

    return;
}

static void
set_redis_record(struct module *mod, double st_array[],
                 U_64 pre_array[], U_64 cur_array[], int inter) {
    int i;
    /* set st record */
    for (i = 0; i < mod->n_col; i++) {
        st_array[i] = cur_array[i];
    }
}

/* register mod to tsar */
void
mod_register(struct module *mod) {
    register_mod_fields(mod, "--redis", redis_usage, redis_info, 3, read_redis_stats, set_redis_record);
}
