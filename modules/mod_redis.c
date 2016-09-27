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


#define MAX_INSTANCES 30
#define LEN_4096 4096

static const char *redis_usage = "    --redis             redis information";

struct redis_instance_info {
    int port;
} redis_instances[MAX_INSTANCES];

/*
 * temp structure for collection information.
 */
struct stats_redis {
    unsigned long long keys;                /* keys in db */
    unsigned long long expires;             /* expire keys in db */
    unsigned long long blocked_clients;     /* */
    unsigned long long connected_clients;   /* */
    unsigned long long expired;             /* */
    unsigned long long evicted;             /* */
    unsigned long long hits;                /* */
    unsigned long long misses;              /* */
    unsigned long long ops;                 /* */
    unsigned long long input;               /* */
    unsigned long long output;              /* */
    unsigned long long mem_rss;             /* */
    unsigned long long mem_peak;            /* */
} redis_stats[MAX_INSTANCES];

unsigned int n_instances = 0;                /* Number of instances */

/* Structure for tsar */
static struct mod_info redis_info[] = {
        {"  keys", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"  exps", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {" block", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"connet", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"expire", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {" evict", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"hirate", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"   ops", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"    in", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"   out", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"   rss", SUMMARY_BIT, MERGE_SUM, STATS_NULL},
        {"  peak", SUMMARY_BIT, MERGE_SUM, STATS_NULL}
};

int redis_inst_initialize() {
    // TODO use ss api to get redis ports
    char *cmd = "ss -4ntlp 2>/dev/null |awk '{split($4,port,\":\"); "
            "if($NF~/redis-server/) print port[length(port)]}' 2>/dev/null";
    char buf[LEN_4096];
    FILE *fp;

    if ((fp = popen(cmd, "r")) == NULL) {
        printf("Error opening pipe!\n");
        return -1;
    }

    int i = 0;
    while (fgets(buf, LEN_4096, fp) != NULL) {
        redis_instances[i].port = atoi(buf);
        if (++i >= MAX_INSTANCES) {
            break;
        }
    }
    n_instances = i;

    if (pclose(fp)) {
        printf("Command not found or exited with error status\n");
        return -1;
    }

    return 0;
}

void collect_redis_inst_stat(int index) {
    int addr_len, domain, sockfd;
    void *addr;
    char buf[LEN_4096], request[LEN_4096], line[LEN_4096];
    FILE *stream = NULL;
    struct sockaddr_in servaddr;

    char *host = "localhost";
    int port = redis_instances[index].port;

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

    if ((connect(sockfd, (struct sockaddr *) addr, addr_len)) == -1) {
        goto writebuf;
    }

    sprintf(request, "%s", "*1\r\n$4\r\ninfo\r\n");
//    sprintf(request, "%s", "*2\r\n$4\r\ninfo\r\n$3\r\nall\r\n");

    if ((write(sockfd, request, strlen(request))) == -1) {
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

    unsigned long long keys = 0, cur_keys,
            expires = 0, cur_expires,
            blocked, connected,
            expired_keys = 0, evicted_keys = 0,
            hits = 0, misses = 0,
            ops = 0, input = 0, output = 0,
            mem_rss = 0, mem_peak = 0,
            db, avg_ttl;

    int keyspace_field = 0;

    // read all content of bulk replay
    // including the last CRLF
    int read_len = 0;
    while (fgets(line, LEN_4096, stream) != NULL) {
        if (!strncmp(line, "# Keyspace", sizeof("# Keyspace") - 1)) {
            keyspace_field = 1;
        } else if (!strncmp(line, "db", sizeof("db") - 1)) {
            if (1 == keyspace_field) {
                // format --> db0:keys=3,expires=0,avg_ttl=0
                sscanf(line, "db%llu:keys=%llu,expires=%llu,avg_ttl=%llu",
                       &db, &cur_keys, &cur_expires, &avg_ttl);
                keys += cur_keys;
                expires += cur_expires;
            }
        } else if (!strncmp(line, "blocked_clients", sizeof("blocked_clients") - 1)) {
            sscanf(line, "blocked_clients:%llu", &blocked);
        } else if (!strncmp(line, "connected_clients", sizeof("connected_clients") - 1)) {
            sscanf(line, "connected_clients:%llu", &connected);
        } else if (!strncmp(line, "expired_keys", sizeof("expired_keys") - 1)) {
            sscanf(line, "expired_keys:%llu", &expired_keys);
        } else if (!strncmp(line, "evicted_keys", sizeof("evicted_keys") - 1)) {
            sscanf(line, "evicted_keys:%llu", &evicted_keys);
        } else if (!strncmp(line, "keyspace_hits", sizeof("keyspace_hits") - 1)) {
            sscanf(line, "keyspace_hits:%llu", &hits);
        } else if (!strncmp(line, "keyspace_misses", sizeof("keyspace_misses") - 1)) {
            sscanf(line, "keyspace_misses:%llu", &misses);
        } else if (!strncmp(line, "instantaneous_ops_per_sec", sizeof("instantaneous_ops_per_sec") - 1)) {
            sscanf(line, "instantaneous_ops_per_sec:%llu", &ops);
        } else if (!strncmp(line, "instantaneous_input_kbps", sizeof("instantaneous_input_kbps") - 1)) {
            sscanf(line, "instantaneous_input_kbps:%llu", &input);
        } else if (!strncmp(line, "instantaneous_output_kbps", sizeof("instantaneous_output_kbps") - 1)) {
            sscanf(line, "instantaneous_output_kbps:%llu", &output);
        } else if (!strncmp(line, "used_memory_rss", sizeof("used_memory_rss") - 1)) {
            sscanf(line, "used_memory_rss:%llu", &mem_rss);
        } else if (!strncmp(line, "used_memory_peak", sizeof("used_memory_peak") - 1)) {
            sscanf(line, "used_memory_peak:%llu", &mem_peak);
        }

        // update read len
        read_len += strlen(line);
        if (read_len == 2 + length) {
            break;
        }
    }

    redis_stats[index].keys = keys;
    redis_stats[index].expires = expires;
    redis_stats[index].blocked_clients = blocked;
    redis_stats[index].connected_clients = connected;
    redis_stats[index].expired = expired_keys;
    redis_stats[index].evicted = evicted_keys;
    redis_stats[index].hits = hits;
    redis_stats[index].misses = misses;
    redis_stats[index].ops = ops;
    redis_stats[index].input = input;
    redis_stats[index].output = output;
    redis_stats[index].mem_rss = mem_rss;
    redis_stats[index].mem_peak = mem_peak;

    writebuf:
    if (stream) {
        fclose(stream);
    }

    if (sockfd != -1) {
        close(sockfd);
    }
}

static void print_instance_stats(struct module *mod) {
    int pos = 0;
    char buf[LEN_1M];
    memset(buf, 0, LEN_1M);
    unsigned int i;

    for (i = 0; i < n_instances; i++) {
        pos += snprintf(buf + pos, LEN_1M - pos, "%d=%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%llu,%d" ITEM_SPLIT,
                        redis_instances[i].port,
                        redis_stats[i].keys,
                        redis_stats[i].expires,
                        redis_stats[i].blocked_clients,
                        redis_stats[i].connected_clients,
                        redis_stats[i].expired,
                        redis_stats[i].evicted,
                        redis_stats[i].hits,
                        redis_stats[i].misses,
                        redis_stats[i].ops,
                        redis_stats[i].input,
                        redis_stats[i].output,
                        redis_stats[i].mem_rss,
                        redis_stats[i].mem_peak,
                        pos);

        if (strlen(buf) == LEN_1M - 1) {
            return;
        }
    }

    set_mod_record(mod, buf);
    return;
}

static void
read_redis_stats(struct module *mod, const char *parameter) {
    // get instance num and port
    if (0 != redis_inst_initialize()) {
        return;
    }

    // collect stat of all redis instances
    unsigned int i = 0;
    for (i = 0; i < n_instances; i++) {
        collect_redis_inst_stat(i);
    }

    print_instance_stats(mod);

    return;
}

static void
set_redis_record(struct module *mod, double st_array[],
                 U_64 pre_array[], U_64 cur_array[], int inter) {
    int i;

    /* st_array is used to display, refer to redis_info */
    /* pre_array/cur_array is used to record stat, refer to redis_stat */

    for (i = 0; i <= 3; i++) {
        st_array[i] = cur_array[i];
    }

    for (i = 4; i <= 5; i++) {
        st_array[i] = (cur_array[i] - pre_array[i]) / (inter * 1.0);
    }

    double hit_qps = (cur_array[6] - pre_array[6]) / (inter * 1.0);
    double miss_qps = (cur_array[7] - pre_array[7]) / (inter * 1.0);
    double total_qps = hit_qps + miss_qps;

    st_array[6] = (0 == total_qps) ? 0 : hit_qps / total_qps;

    for (i = 8; i <= 12; i++) {
        st_array[i - 1] = cur_array[i];
    }

}

/* register mod to tsar */
void
mod_register(struct module *mod) {
    register_mod_fields(mod, "--redis", redis_usage, redis_info, 14, read_redis_stats, set_redis_record);
}
