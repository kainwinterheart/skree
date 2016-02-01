#include <map>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundef"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wc++98-compat"
#pragma clang diagnostic ignored "-Wpadded"
#include <ev.h>
#pragma clang diagnostic pop

#include <list>
#include <ctime>
#include <deque>
#include <queue>
#include <string>
#include <vector>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <utility>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <algorithm>
#include <pthread.h>
#include <strings.h>

#include "db_wrapper.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unordered_map>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated"
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wdocumentation-unknown-command"
#pragma clang diagnostic ignored "-Wmissing-noreturn"
#pragma clang diagnostic ignored "-Wweak-vtables"
#include "tclap/CmdLine.h"
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++98-compat"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#pragma clang diagnostic ignored "-Wdeprecated"
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wextra-semi"
#pragma clang diagnostic ignored "-Wundef"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wdisabled-macro-expansion"
#pragma clang diagnostic ignored "-Wpadded"
#pragma clang diagnostic ignored "-Wweak-vtables"
#include "yaml-cpp/yaml.h"
#pragma clang diagnostic pop

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#pragma clang diagnostic ignored "-Wc++98-compat"
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#pragma clang diagnostic ignored "-Wglobal-constructors"
#pragma clang diagnostic ignored "-Wshadow"
#pragma clang diagnostic ignored "-Wold-style-cast"

// thank you, stackoverflow!
#ifndef htonll
#define htonll(x) ((1 == htonl(1)) ? (x) : ((uint64_t)htonl(\
    (x) & 0xFFFFFFFF) << 32) | htonl((x) >> 32))
#endif

#ifndef ntohll
#define ntohll(x) ((1 == ntohl(1)) ? (x) : ((uint64_t)ntohl(\
    (x) & 0xFFFFFFFF) << 32) | ntohl((x) >> 32))
#endif

class Client;

struct new_client_t {
    int fh;
    void (*cb) (Client*);
    sockaddr_in* s_in;
    socklen_t s_in_len;
};

static std::queue<new_client_t*> new_clients;
static pthread_mutex_t new_clients_mutex;

struct char_pointer_comparator : public std::binary_function<char*, char*, bool> {
    bool operator()(const char* a, const char* b) const {
        return (strcmp(a, b) == 0);
    }
};

struct char_pointer_hasher {
    //BKDR hash algorithm
    int operator()(char* key) const {
        int seed = 131; //31 131 1313 13131131313 etc//
        int hash = 0;
        size_t len = strlen(key);

        for(size_t i = 0; i < len; ++i) {
            hash = ((hash * seed) + key[i]);
        }

        return (hash & 0x7FFFFFFF);
    }
};

struct skree_module_t {
    size_t path_len;
    char* path;
    const void* config;
};

struct event_group_t {
    size_t name_len;
    char* name;
    skree_module_t* module;
};

struct known_event_t {
    uint32_t id_len;
    uint32_t id_len_net;
    char* id;
    event_group_t* group;
    uint32_t ttl;
    uint32_t id_len_size;
};

typedef std::unordered_map<char*, skree_module_t*, char_pointer_hasher, char_pointer_comparator> skree_modules_t;
static skree_modules_t skree_modules;

typedef std::unordered_map<char*, event_group_t*, char_pointer_hasher, char_pointer_comparator> event_groups_t;
static event_groups_t event_groups;

typedef std::unordered_map<char*, known_event_t*, char_pointer_hasher, char_pointer_comparator> known_events_t;
static known_events_t known_events;

typedef std::unordered_map<char*, uint64_t, char_pointer_hasher, char_pointer_comparator> failover_t;
static failover_t failover;

typedef std::unordered_map<uint64_t, uint64_t> wip_t;
static wip_t wip;

typedef std::unordered_map<char*, uint64_t, char_pointer_hasher, char_pointer_comparator> no_failover_t;
static no_failover_t no_failover;

typedef std::unordered_map<char*, Client*, char_pointer_hasher, char_pointer_comparator> known_peers_t;
static known_peers_t known_peers;
static known_peers_t known_peers_by_conn_id;
static pthread_mutex_t known_peers_mutex;

typedef std::unordered_map<char*, bool, char_pointer_hasher, char_pointer_comparator> me_t;
static me_t me;
static pthread_mutex_t me_mutex;

struct peer_to_discover_t {
    const char* host;
    uint32_t port;
};

typedef std::unordered_map<char*, peer_to_discover_t*, char_pointer_hasher, char_pointer_comparator> peers_to_discover_t;
static peers_to_discover_t peers_to_discover;
static pthread_mutex_t peers_to_discover_mutex;

struct client_bound_ev_io {
    ev_io watcher;
    Client* client;
};

struct PendingReadCallbackArgs {
    size_t len;
    size_t* out_len;
    char* data;
    char** out_data;
    bool* stop;
    void* ctx;
};

struct PendingReadsQueueItem {
    size_t len;
    PendingReadsQueueItem* (Client::* cb) (PendingReadCallbackArgs*);
    void (*err) (void*);
    void* ctx;
    bool opcode;
};

struct WriteQueueItem {
    size_t len;
    size_t pos;
    char* data;
    PendingReadsQueueItem* cb;
};

struct muh_str_t {
    size_t len;
    char* data;
};

struct out_packet_i_ctx {
    pthread_mutex_t* mutex;
    known_event_t* event;
    muh_str_t* data;
    muh_str_t* peer_id;
    uint64_t wrinseq;
    char* failover_key;
    uint64_t failover_key_len;
    uint32_t* count_replicas;
    uint32_t* pending;
    uint32_t* acceptances;
    char* rpr;
    uint64_t rid;
    uint32_t peers_cnt;
};

struct out_data_c_ctx {
    Client* client;
    known_event_t* event;
    muh_str_t* rin;
    muh_str_t* rpr;
    uint64_t rid;
    uint64_t wrinseq;
    uint64_t failover_key_len;
    char* failover_key;
};

struct in_packet_c_ctx {
    size_t event_name_len;
    char* event_name;
    uint64_t rid;
    char* rin;
    uint32_t rin_len;
};

static std::queue<out_packet_i_ctx*> replication_exec_queue;
static pthread_mutex_t replication_exec_queue_mutex;

static void client_cb(struct ev_loop* loop, ev_io* _watcher, int events);

static inline char* make_peer_id(size_t peer_name_len, char* peer_name, uint32_t peer_port) {
    char* peer_id = (char*)malloc(peer_name_len
        + 1 // :
        + 5 // port string
        + 1 // \0
    );

    memcpy(peer_id, peer_name, peer_name_len);
    sprintf(peer_id + peer_name_len, ":%u", peer_port);

    return peer_id;
}

static inline char* get_host_from_sockaddr_in(const sockaddr_in* s_in) {
    char* conn_name = NULL;

    if(s_in->sin_family == AF_INET) {
        conn_name = (char*)malloc(INET_ADDRSTRLEN);
        inet_ntop(AF_INET, &(s_in->sin_addr), conn_name, INET_ADDRSTRLEN);

    } else {
        conn_name = (char*)malloc(INET6_ADDRSTRLEN);
        inet_ntop(AF_INET6, &(((sockaddr_in6*)s_in)->sin6_addr), conn_name, INET6_ADDRSTRLEN);
    }

    return conn_name;
}

static inline uint32_t get_port_from_sockaddr_in(const sockaddr_in* s_in) {
    if(s_in->sin_family == AF_INET) {
        return ntohs(s_in->sin_port);

    } else {
        return ntohs(((sockaddr_in6*)s_in)->sin6_port);
    }
}

static inline void save_peers_to_discover() {
    pthread_mutex_lock(&peers_to_discover_mutex);

    size_t cnt = htonll(peers_to_discover.size());
    size_t dump_len = 0;
    char* dump = (char*)malloc(sizeof(cnt));

    memcpy(dump + dump_len, &cnt, sizeof(cnt));
    dump_len += sizeof(cnt);

    for(
        peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
        it != peers_to_discover.cend();
        ++it
    ) {
        peer_to_discover_t* peer = it->second;

        size_t len = strlen(peer->host);
        uint32_t port = htonl(peer->port);

        dump = (char*)realloc(dump,
            dump_len
            + sizeof(len)
            + len
            + sizeof(port)
        );

        size_t _len = htonll(len);
        memcpy(dump + dump_len, &_len, sizeof(_len));
        dump_len += sizeof(_len);

        memcpy(dump + dump_len, peer->host, len);
        dump_len += len;

        memcpy(dump + dump_len, &port, sizeof(port));
        dump_len += sizeof(port);
    }

    pthread_mutex_unlock(&peers_to_discover_mutex);

    const char* key = "peers_to_discover";
    const size_t key_len = strlen(key);

    if(!db.set(key, key_len, dump, dump_len))
        fprintf(stderr, "Failed to save peers list: %s\n", db.error().name());
}

static inline void load_peers_to_discover() {
    const char* key = "peers_to_discover";
    const size_t key_len = strlen(key);
    size_t value_len;

    char* value = db.get(key, key_len, &value_len);

    if(value != NULL) {
        size_t offset = 0;

        size_t cnt;
        memcpy(&cnt, value + offset, sizeof(cnt));
        cnt = ntohll(cnt);
        offset += sizeof(cnt);

        while(cnt > 0) {
            size_t hostname_len;
            memcpy(&hostname_len, value + offset, sizeof(hostname_len));
            hostname_len = ntohll(hostname_len);
            offset += sizeof(hostname_len);

            char* hostname = (char*)malloc(hostname_len + 1);
            memcpy(hostname, value + offset, hostname_len);
            hostname[hostname_len] = '\0';
            offset += hostname_len;

            uint32_t port;
            memcpy(&port, value + offset, sizeof(port));
            port = ntohl(port);
            offset += sizeof(port);

            char* peer_id = make_peer_id(hostname_len, hostname, port);

            peers_to_discover_t::const_iterator it = peers_to_discover.find(peer_id);

            if(it == peers_to_discover.cend()) {
                peer_to_discover_t* peer = (peer_to_discover_t*)malloc(sizeof(*peer));

                peer->host = hostname;
                peer->port = port;

                peers_to_discover[peer_id] = peer;

            } else {
                free(peer_id);
                free(hostname);
            }

            --cnt;
        }
    }
}

static inline void begin_replication(out_packet_r_ctx*& r_ctx);
static inline void continue_replication_exec(out_packet_i_ctx*& ctx);
static inline short save_event(
    in_packet_e_ctx* ctx,
    uint32_t replication_factor,
    Client* client,
    std::vector<uint64_t>* task_ids
);
static inline short repl_save(
    in_packet_r_ctx* ctx,
    Client* client
);
static inline void repl_clean(
    size_t failover_key_len,
    const char* failover_key,
    uint64_t wrinseq
);

static inline void unfailover(char* failover_key) {
    {
        failover_t::const_iterator it = failover.find(failover_key);

        if(it != failover.cend())
            failover.erase(it);
    }

    {
        no_failover_t::const_iterator it = no_failover.find(failover_key);

        if(it != no_failover.cend())
            no_failover.erase(it);
    }
}

static inline void continue_replication_exec(out_packet_i_ctx*& ctx) {
    if(*(ctx->pending) == 0) {
        pthread_mutex_lock(&replication_exec_queue_mutex);

        replication_exec_queue.push(ctx);

        pthread_mutex_unlock(&replication_exec_queue_mutex);
    }
}

static void client_cb(struct ev_loop* loop, ev_io* _watcher, int events) {
    struct client_bound_ev_io* watcher = (struct client_bound_ev_io*)_watcher;
    Client* client = watcher->client;

    if(events & EV_ERROR) {
        printf("EV_ERROR!\n");

        delete client;

        return;
    }

    if(events & EV_READ) {
        char* buf = (char*)malloc(read_size);
        int read = recv(_watcher->fd, buf, read_size, 0);

        if(read > 0) {
            // for(int i = 0; i < read; ++i)
            //     printf("read from %s: 0x%.2X\n", client->get_peer_id(),buf[i]);

            client->push_read_queue(read, buf);
            free(buf);

        } else if(read < 0) {
            if((errno != EAGAIN) && (errno != EINTR)) {
                perror("recv");
                free(buf);
                delete client;
                return;
            }

        } else {
            free(buf);
            delete client;
            return;
        }
    }

    if(events & EV_WRITE) {
        WriteQueueItem* item = client->get_pending_write();

        if(item != NULL) {
            int written = write(
                _watcher->fd,
                (item->data + item->pos),
                (item->len - item->pos)
            );

            if(written < 0) {
                if((errno != EAGAIN) && (errno != EINTR)) {
                    perror("write");
                    delete client;
                    return;
                }

            } else {
                // for(int i = 0; i < written; ++i)
                //     printf("written to %s: 0x%.2X\n", client->get_peer_id(),((char*)(item->data + item->pos))[i]);

                item->pos += written;

                if((item->pos >= item->len) && (item->cb != NULL)) {
                    client->push_pending_reads_queue(item->cb);
                    item->cb = NULL;
                }
            }
        }
    }

    return;
}

static void* client_thread(void* args) {
    struct ev_loop* loop = ev_loop_new(0);

    while(true) {
        ev_run(loop, EVRUN_NOWAIT);

        if(!new_clients.empty()) {
            pthread_mutex_lock(&new_clients_mutex);

            if(!new_clients.empty()) {
                new_client_t* new_client = new_clients.front();
                new_clients.pop();

                pthread_mutex_unlock(&new_clients_mutex);

                Client* client = new Client(
                    new_client->fh,
                    loop,
                    new_client->s_in,
                    new_client->s_in_len
                );

                if(new_client->cb != NULL)
                    new_client->cb(client);

                free(new_client);

            } else {
                pthread_mutex_unlock(&new_clients_mutex);
            }
        }
    }

    return NULL;
}

static void discovery_cb1(Client* client) {
    auto w_req = Actions::W::out_init();

    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(sizeof(*item));

    item->len = 1;
    item->cb = &Client::discovery_cb2;
    item->ctx = NULL;
    item->err = NULL;
    item->opcode = true;

    client->push_write_queue(w_req->len, w_req->data, item);
}

static void* replication_exec_thread(void* args) {
    while(true) {
        if(replication_exec_queue.size() == 0) continue;

        pthread_mutex_lock(&replication_exec_queue_mutex);

        // TODO: persistent queue
        out_packet_i_ctx* ctx = replication_exec_queue.front();
        replication_exec_queue.pop();

        pthread_mutex_unlock(&replication_exec_queue_mutex);

        // printf("Replication exec thread for task %llu\n", ctx->rid);

        if(ctx->acceptances == ctx->count_replicas) {
            {
                failover_t::const_iterator it = failover.find(ctx->failover_key);

                if(it == failover.cend()) {
                    // TODO: cleanup
                    continue;
                }
            }

            {
                no_failover_t::const_iterator it = no_failover.find(ctx->failover_key);

                if(it != no_failover.cend()) {
                    if((it->second + no_failover_time) > std::time(nullptr)) {
                        // TODO: cleanup
                        continue;

                    } else {
                        no_failover.erase(it);
                    }
                }
            }

            in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(sizeof(*event));

            event->len = ctx->data->len;
            event->data = ctx->data->data;

            in_packet_e_ctx* e_ctx = (in_packet_e_ctx*)malloc(sizeof(*e_ctx));

            e_ctx->cnt = 1;
            e_ctx->event_name_len = ctx->event->id_len;
            e_ctx->event_name = ctx->event->id;
            e_ctx->events = new std::list<in_packet_e_ctx_event*>();
            e_ctx->events->push_back(event);

            std::vector<uint64_t> task_ids;
            save_event(e_ctx, 0, NULL, &task_ids);

            Client::free_in_packet_e_ctx((void*)e_ctx);

            failover[ctx->failover_key] = task_ids.front();

            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                ctx->wrinseq
            );

            if(ctx->rpr != NULL) {
                // const muh_str_t*& peer_id, const known_event_t*& event,
                // const uint64_t& rid
                auto x_req = Actions::X::out_init(ctx->peer_id, ctx->event, ctx->rid);
                size_t offset = 0;

                while(ctx->peers_cnt > 0) {
                    size_t peer_id_len = strlen(ctx->rpr + offset);
                    char* peer_id = (char*)malloc(peer_id_len + 1);
                    memcpy(peer_id, ctx->rpr + offset, peer_id_len);
                    peer_id[peer_id_len] = '\0';
                    offset += peer_id_len + 1;

                    known_peers_t::const_iterator it = known_peers.find(peer_id);

                    if(it != known_peers.cend()) {
                        it->second->push_write_queue(x_req->len, x_req->data, NULL);
                    }

                    --(ctx->peers_cnt);
                }
            }

        } else {
            // TODO: think about repl_clean()
            repl_clean(
                ctx->failover_key_len,
                ctx->failover_key,
                ctx->wrinseq
            );

            unfailover(ctx->failover_key);

            free(ctx->data->data);
            free(ctx->data);
        }

        pthread_mutex_destroy(ctx->mutex);

        free(ctx->mutex);
        free(ctx->acceptances);
        free(ctx->pending);
        free(ctx->count_replicas);
        free(ctx);
    }

    return NULL;
}

static void* replication_thread(void* args) {
    while(true) {
        std::vector<muh_str_t*> peer_ids;

        pthread_mutex_lock(&peers_to_discover_mutex);

        for(
            peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
            it != peers_to_discover.cend();
            ++it
        ) {
            muh_str_t* item = (muh_str_t*)malloc(sizeof(*item));

            item->len = strlen(it->first);
            item->data = it->first;

            peer_ids.push_back(item);
        }

        pthread_mutex_unlock(&peers_to_discover_mutex);

        std::random_shuffle(peer_ids.begin(), peer_ids.end());

        known_peers_t::const_iterator _peer;
        Client* peer;
        get_keys_result_t* dbdata;
        std::vector<std::string> keys;
        uint64_t now = std::time(nullptr);

        for(
            known_events_t::const_iterator _event = known_events.cbegin();
            _event != known_events.cend();
            ++_event
        ) {
            known_event_t* event = _event->second;
            // printf("repl thread: %s\n", event->id);

            for(
                std::vector<muh_str_t*>::const_iterator _peer_id = peer_ids.cbegin();
                _peer_id != peer_ids.cend();
                ++_peer_id
            ) {
                size_t suffix_len =
                    event->id_len
                    + 1 // :
                    + (*_peer_id)->len
                ;

                char* suffix = (char*)malloc(
                    suffix_len
                    + 1 // :
                    + 20 // wrinseq
                    + 1 // \0
                );
                sprintf(suffix, "%s:%s", event->id, (*_peer_id)->data);
                // printf("repl thread: %s\n", suffix);

                std::string wrinseq_key("wrinseq:", 8);
                wrinseq_key.append(suffix, suffix_len);

                std::string rinseq_key("rinseq:", 7);
                rinseq_key.append(suffix, suffix_len);

                keys.push_back(wrinseq_key);
                keys.push_back(rinseq_key);
// printf("replication_thread: before first db_get_keys\n");
                dbdata = db_get_keys(keys);
                // printf("replication_thread: after first db_get_keys\n");
                keys.clear();

                if(dbdata == NULL) {
                    fprintf(stderr, "db.accept_bulk failed: %s\n", db.error().name());
                    exit(1);
                }

                uint64_t rinseq;
                uint64_t wrinseq;

                auto next = [&wrinseq_key, &wrinseq](){
                    uint64_t next_wrinseq = htonll(wrinseq + 1);
                    uint64_t __wrinseq = htonll(wrinseq);
// printf("replication_thread: before db.cas()\n");
                    if(!db.cas(
                        wrinseq_key.c_str(),
                        wrinseq_key.length(),
                        (char*)&__wrinseq,
                        sizeof(__wrinseq),
                        (char*)&next_wrinseq,
                        sizeof(next_wrinseq)
                    )) {
                        fprintf(stderr, "db.cas(%s,%llu,%llu) failed: %s\n", wrinseq_key.c_str(), wrinseq, ntohll(next_wrinseq), db.error().name());
                        exit(1);
                    }
// printf("replication_thread: after db.cas()\n");
                };

                {
                    uint64_t* _rinseq = parse_db_value<uint64_t>(dbdata, &rinseq_key);
                    uint64_t* _wrinseq = parse_db_value<uint64_t>(dbdata, &wrinseq_key);

                    if(_rinseq == NULL) rinseq = 0;
                    else {
                        rinseq = ntohll(*_rinseq);
                        free(_rinseq);
                    }

                    if(_wrinseq == NULL) {
                        wrinseq = 0;
                        uint64_t __wrinseq = htonll(wrinseq);

                        if(!db.add(
                            wrinseq_key.c_str(),
                            wrinseq_key.length(),
                            (char*)&__wrinseq,
                            sizeof(__wrinseq)
                        )) {
                            auto error = db.error();
                            fprintf(stderr, "db.add(%s) failed: %s\n", wrinseq_key.c_str(), error.name());

                            if(error.code() == kyotocabinet::BasicDB::Error::Code::DUPREC) {
                                next();
                                continue;

                            } else {
                                exit(1);
                            }
                        }

                    } else {
                        wrinseq = ntohll(*_wrinseq);
                        free(_wrinseq);
                    }
                }

                delete dbdata;

                if(wrinseq >= rinseq) {
                    // printf("Skip repl: %llu >= %llu\n", wrinseq, rinseq);
                    free(suffix);
                    continue;
                }

                suffix[suffix_len] = ':';
                ++suffix_len;

                sprintf(suffix + suffix_len, "%llu", wrinseq);
                suffix_len += 20;
                size_t suffix_slen = strlen(suffix);

                std::string rin_key("rin:", 4);
                rin_key.append(suffix, suffix_slen);

                std::string rts_key("rts:", 4);
                rts_key.append(suffix, suffix_slen);

                std::string rid_key("rid:", 4);
                rid_key.append(suffix, suffix_slen);

                std::string rpr_key("rpr:", 4);
                rpr_key.append(suffix, suffix_slen);

                keys.push_back(rin_key);
                keys.push_back(rts_key);
                keys.push_back(rid_key);
                keys.push_back(rpr_key);

                // for(
                //     std::vector<std::string>::const_iterator it = keys.cbegin();
                //     it != keys.cend();
                //     ++it
                //) {
                //
                //     printf("gotta ask for (%lu bytes) %s\n", it->size(), it->c_str());
                // }

                dbdata = db_get_keys(keys);
                keys.clear();

                if(dbdata == NULL) {
                    fprintf(stderr, "db.accept_bulk failed: %s\n", db.error().name());
                    exit(1);
                }

                uint32_t rin_len;
                size_t _rin_len;
                char* rin = parse_db_value<char>(dbdata, &rin_key, &_rin_len);
                rin_len = _rin_len;

                if(rin == NULL) {
                    fprintf(stderr, "No data for replicated event: %s, rin_key: %s\n", suffix, rin_key.c_str());
                    next();
                    continue;
                }

                uint64_t rts;
                uint64_t rid;
                uint64_t rid_net;

                {
                    uint64_t* _rts = parse_db_value<uint64_t>(dbdata, &rts_key);
                    uint64_t* _rid = parse_db_value<uint64_t>(dbdata, &rid_key);

                    if(_rts == NULL) {
                        fprintf(stderr, "No timestamp for replicated event: %s\n", suffix);
                        next();
                        continue;

                    } else {
                        rts = ntohll(*_rts);
                        // free(_rts);
                    }

                    if(_rid == NULL) {
                        fprintf(stderr, "No remote id for replicated event: %s\n", suffix);
                        next();
                        continue;

                    } else {
                        rid_net = *_rid;
                        // free(_rid);
                        rid = ntohll(rid_net);
                    }
                }

                size_t rpr_len;
                char* rpr = parse_db_value<char>(dbdata, &rpr_key, &rpr_len);

                delete dbdata;

                if((rts + event->ttl) > now) {
                    // printf("skip repl: not now\n");
                    free(rin);
                    if(rpr != NULL) free(rpr);
                    free(suffix);
                    continue;
                }

                char* failover_key = suffix;
                sprintf(failover_key + suffix_len - 20 - 1, "%llu", rid);

                {
                    failover_t::const_iterator it = failover.find(failover_key);

                    if(it != failover.cend()) {
                        free(rin);
                        if(rpr != NULL) free(rpr);
                        // free(suffix);
                        free(failover_key);
                        continue;
                    }
                }

                {
                    no_failover_t::const_iterator it = no_failover.find(failover_key);

                    if(it != no_failover.cend()) {
                        if((it->second + no_failover_time) > now) {
                            free(rin);
                            if(rpr != NULL) free(rpr);
                            // free(suffix);
                            free(failover_key);
                            continue;

                        } else {
                            no_failover.erase(it);
                        }
                    }
                }

                failover[failover_key] = 0;

                pthread_mutex_lock(&known_peers_mutex);

                _peer = known_peers.find((*_peer_id)->data);

                if(_peer == known_peers.cend()) peer = NULL;
                else peer = _peer->second;

                pthread_mutex_unlock(&known_peers_mutex);

                // printf("Seems like I need to failover task %llu\n", rid);

                if(peer == NULL) {
                    size_t offset = 0;
                    uint32_t peers_cnt = 0;
                    bool have_rpr = false;

                    uint32_t* count_replicas = (uint32_t*)malloc(sizeof(
                        *count_replicas));
                    uint32_t* acceptances = (uint32_t*)malloc(sizeof(
                        *acceptances));
                    uint32_t* pending = (uint32_t*)malloc(sizeof(
                        *pending));

                    *count_replicas = 0;
                    *acceptances = 0;
                    *pending = 0;

                    pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(*mutex));
                    pthread_mutex_init(mutex, NULL);

                    muh_str_t* data_str = (muh_str_t*)malloc(sizeof(*data_str));

                    data_str->len = rin_len;
                    data_str->data = rin;

                    if(rpr != NULL) {
                        uint32_t _peers_cnt;
                        memcpy(&_peers_cnt, rpr + offset, sizeof(_peers_cnt));
                        peers_cnt = ntohl(_peers_cnt);

                        *count_replicas = peers_cnt;

                        auto i_req = Actions::I::out_init(*peer_id, event, rid_net);

                        if(peers_cnt > 0) {
                            have_rpr = true;

                            while(peers_cnt > 0) {
                                size_t peer_id_len = strlen(rpr + offset);
                                char* peer_id = (char*)malloc(peer_id_len + 1);
                                memcpy(peer_id, rpr + offset, peer_id_len);
                                peer_id[peer_id_len] = '\0';
                                offset += peer_id_len + 1;

                                known_peers_t::const_iterator it = known_peers.find(peer_id);

                                if(it == known_peers.cend()) {
                                    ++(*acceptances);

                                } else {
                                    ++(*pending);

                                    out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(
                                        sizeof(*ctx));

                                    ctx->count_replicas = count_replicas;
                                    ctx->pending = pending;
                                    ctx->acceptances = acceptances;
                                    ctx->mutex = mutex;
                                    ctx->event = event;
                                    ctx->data = data_str;
                                    ctx->peer_id = *_peer_id;
                                    ctx->wrinseq = wrinseq;
                                    ctx->failover_key = failover_key;
                                    ctx->failover_key_len = suffix_len;
                                    ctx->rpr = rpr;
                                    ctx->peers_cnt = peers_cnt;
                                    ctx->rid = rid;

                                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                                        sizeof(*item));

                                    item->len = 1;
                                    item->cb = &Client::propose_self_k_cb;
                                    item->ctx = (void*)ctx;
                                    item->err = &Client::propose_self_f_cb;
                                    item->opcode = true;

                                    it->second->push_write_queue(i_req->len, i_req->data, item);
                                }

                                --peers_cnt;
                            }
                        }
                    }

                    if(!have_rpr) {
                        out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(sizeof(*ctx));

                        ctx->count_replicas = count_replicas;
                        ctx->pending = pending;
                        ctx->acceptances = acceptances;
                        ctx->mutex = mutex;
                        ctx->event = event;
                        ctx->data = data_str;
                        ctx->peer_id = *_peer_id;
                        ctx->wrinseq = wrinseq;
                        ctx->failover_key = failover_key;
                        ctx->failover_key_len = suffix_len;
                        ctx->rpr = rpr;
                        ctx->peers_cnt = 0;
                        ctx->rid = rid;

                        replication_exec_queue.push(ctx);
                    }

                } else {
                    auto c_req = Actions::C::out_init(event, rid_net, rin_len, rin);

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                        sizeof(*item));

                    // TODO: rin_str's type
                    muh_str_t* rin_str = (muh_str_t*)malloc(sizeof(*rin_str));

                    rin_str->len = rin_len;
                    rin_str->data = rin;

                    muh_str_t* rpr_str = NULL;

                    if(rpr != NULL) {
                        rpr_str = (muh_str_t*)malloc(sizeof(*rpr_str));
                        rpr_str->len = strlen(rpr);
                        rpr_str->data = rpr;
                    }

                    out_data_c_ctx* ctx = (out_data_c_ctx*)malloc(sizeof(*ctx));

                    ctx->client = peer;
                    ctx->event = event;
                    ctx->rin = rin_str;
                    ctx->rpr = rpr_str;
                    ctx->rid = rid;
                    ctx->wrinseq = wrinseq;
                    ctx->failover_key = failover_key;
                    ctx->failover_key_len = suffix_len;

                    item->len = 1;
                    item->cb = &Client::ping_task_k_cb;
                    item->ctx = (void*)ctx;
                    item->err = &Client::ping_task_f_cb;
                    item->opcode = true;

                    peer->push_write_queue(c_req->len, c_req->data, item);
                }

                next();
            }
        }
    }

    return NULL;
}

static void* discovery_thread(void* args) {
    while(true) {
        for(
            peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
            it != peers_to_discover.cend();
            ++it
        ) {
            peer_to_discover_t* peer_to_discover = it->second;

            addrinfo hints;
            addrinfo* service_info;

            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_NUMERICSERV;

            char port[6];
            sprintf(port, "%d", peer_to_discover->port);
            int rv;

            if((rv = getaddrinfo(peer_to_discover->host, port, &hints, &service_info)) != 0) {
                fprintf(stderr, "getaddrinfo(%s, %u): %s\n",
                    peer_to_discover->host, peer_to_discover->port, gai_strerror(rv));
                continue;
            }

            int fh;
            int yes = 1;
            sockaddr_in* addr = (sockaddr_in*)malloc(sizeof(*addr));
            socklen_t addr_len;
            bool connected = false;

            for(addrinfo* ai_it = service_info; ai_it != NULL; ai_it = ai_it->ai_next) {
                if((fh = socket(ai_it->ai_family, ai_it->ai_socktype, ai_it->ai_protocol)) == -1) {
                    perror("socket");
                    continue;
                }

                if(setsockopt(fh, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(setsockopt(fh, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                timeval tv;
                tv.tv_sec = (discovery_timeout_milliseconds / 1000);
                tv.tv_usec = ((discovery_timeout_milliseconds % 1000) * 1000);

                if(setsockopt(fh, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(setsockopt(fh, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
                    perror("setsockopt");
                    close(fh);
                    continue;
                }

                if(connect(fh, ai_it->ai_addr, ai_it->ai_addrlen) == -1) {
                    if(errno != EINPROGRESS) {
                        perror("connect");
                        close(fh);
                        continue;
                    }

                    fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);
                }

                *addr = *(sockaddr_in*)(ai_it->ai_addr);
                addr_len = ai_it->ai_addrlen;
                connected = true;

                break;
            }

            freeaddrinfo(service_info);

            if(connected) {
                if(getpeername(fh, (sockaddr*)addr, &addr_len) == -1) {
                    perror("getpeername");
                    close(fh);
                    free(addr);

                } else {
                    char* conn_name = get_host_from_sockaddr_in(addr);
                    uint32_t conn_port = get_port_from_sockaddr_in(addr);
                    char* conn_id = make_peer_id(strlen(conn_name), conn_name, conn_port);

                    free(conn_name);
                    bool found = false;

                    {
                        pthread_mutex_lock(&known_peers_mutex);

                        known_peers_t::const_iterator it =
                            known_peers_by_conn_id.find(conn_id);

                        if(it != known_peers_by_conn_id.cend())
                            found = true;

                        pthread_mutex_unlock(&known_peers_mutex);
                    }

                    if(!found) {
                        pthread_mutex_lock(&known_peers_mutex);

                        known_peers_t::const_iterator it = known_peers.find(conn_id);

                        if(it != known_peers.cend())
                            found = true;

                        pthread_mutex_unlock(&known_peers_mutex);
                    }

                    if(!found) {
                        pthread_mutex_lock(&me_mutex);

                        me_t::const_iterator it = me.find(conn_id);

                        if(it != me.cend())
                            found = true;

                        pthread_mutex_unlock(&me_mutex);
                    }

                    free(conn_id);

                    if(found) {
                        shutdown(fh, SHUT_RDWR);
                        close(fh);
                        free(addr);

                    } else {
                        new_client_t* new_client = (new_client_t*)malloc(
                            sizeof(*new_client));

                        new_client->fh = fh;
                        new_client->cb = discovery_cb1;
                        new_client->s_in = addr;
                        new_client->s_in_len = addr_len;

                        pthread_mutex_lock(&new_clients_mutex);
                        new_clients.push(new_client);
                        pthread_mutex_unlock(&new_clients_mutex);
                    }
                }

            } else {
                fprintf(stderr, "Peer %s is unreachable\n", it->first);
                free(addr);
            }
        }

        sleep(5);
    }

    return NULL;
}

static void socket_cb(struct ev_loop* loop, ev_io* watcher, int events) {
    sockaddr_in* addr = (sockaddr_in*)malloc(sizeof(*addr));
    socklen_t len = sizeof(*addr);

    int fh = accept(watcher->fd, (sockaddr*)addr, &len);

    if(fh < 0) {
        perror("accept");
        free(addr);
        return;
    }

    new_client_t* new_client = (new_client_t*)malloc(sizeof(*new_client));

    new_client->fh = fh;
    new_client->cb = NULL;
    new_client->s_in = addr;
    new_client->s_in_len = len;

    pthread_mutex_lock(&new_clients_mutex);
    new_clients.push(new_client);
    pthread_mutex_unlock(&new_clients_mutex);

    return;
}

static void* synchronization_thread(void* args) {
    while(true) {
        sleep(1);
        db.synchronize();

        pthread_mutex_lock(&stat_mutex);

        if(stat_num_inserts > 0)
            printf("number of inserts for last second: %llu\n", stat_num_inserts);

        if(stat_num_replications > 0)
            printf("number of replication inserts for last second: %llu\n",
                stat_num_replications);

        stat_num_inserts = 0;
        stat_num_replications = 0;

        pthread_mutex_unlock(&stat_mutex);
    }

    return NULL;
}

int main(int argc, char** argv) {
    std::string db_file_name;
    std::string known_events_file_name;

    try {
        TCLAP::CmdLine cmd("skree", '=', "0.01");

        TCLAP::ValueArg<uint32_t> _port(
            "", // short param name
            "port", // long param name
            "Server port", // long description
            true, // required
            0,
            "server_port" // human-readable parameter title
        );

        TCLAP::ValueArg<uint32_t> _max_client_threads(
            "",
            "client-threads",
            "Client threads",
            false,
            max_client_threads,
            "thread_count"
        );

        TCLAP::ValueArg<std::string> _db_file_name(
            "",
            "db",
            "Database file",
            true,
            "",
            "file"
        );

        TCLAP::ValueArg<std::string> _known_events_file_name(
            "",
            "events",
            "Known events file",
            true,
            "",
            "file"
        );

        cmd.add(_port);
        cmd.add(_max_client_threads);
        cmd.add(_db_file_name);
        cmd.add(_known_events_file_name);

        cmd.parse(argc, argv);

        my_port = _port.getValue();
        max_client_threads = _max_client_threads.getValue();
        db_file_name = _db_file_name.getValue();
        known_events_file_name = _known_events_file_name.getValue();

    } catch(TCLAP::ArgException& e) {
        printf("%s %s\n", e.error().c_str(), e.argId().c_str());
    }

    YAML::Node config = YAML::LoadFile(known_events_file_name);

    {
        if(config.Type() != YAML::NodeType::Sequence) {
            fprintf(stderr, "Known events file should contain a sequence of event groups\n");
        }

        for(YAML::const_iterator group = config.begin(); group != config.end(); ++group) {
            if(group->Type() != YAML::NodeType::Map) {
                fprintf(stderr, "Each event group should be a map\n");
                exit(1);
            }

            const YAML::Node _name = (*group)["name"];
            std::string group_name;

            if(_name && (_name.Type() == YAML::NodeType::Scalar)) {
                group_name = _name.as<std::string>();

            } else {
                fprintf(stderr, "Every event group should have a name\n");
                exit(1);
            }

            const YAML::Node _events = (*group)["events"];

            if(!_events || (_events.Type() != YAML::NodeType::Sequence)) {
                fprintf(stderr, "Every event group should have an event list\n");
                exit(1);
            }

            event_group_t* event_group = (event_group_t*)malloc(sizeof(*event_group));

            event_group->name_len = group_name.length();

            char* group_name_ = (char*)malloc(event_group->name_len + 1);
            memcpy(group_name_, group_name.c_str(), event_group->name_len);
            group_name_[event_group->name_len] = '\0';

            event_group->name = group_name_;
            // event_group->module = skree_module; // TODO

            event_groups_t::const_iterator it = event_groups.find(group_name_);

            if(it == event_groups.cend()) {
                event_groups[group_name_] = event_group;

            } else {
                fprintf(stderr, "Duplicate group name: %s\n", group_name_);
                exit(1);
            }

            for(
                YAML::const_iterator event = _events.begin();
                event != _events.end();
                ++event
            ) {
                if(event->Type() != YAML::NodeType::Map) {
                    fprintf(stderr, "Every event should be a map\n");
                    exit(1);
                }

                const YAML::Node _id = (*event)["id"];

                if(_id && (_id.Type() == YAML::NodeType::Scalar)) {
                    const YAML::Node _ttl = (*event)["ttl"];
                    uint32_t ttl;

                    if(_ttl && (_ttl.Type() == YAML::NodeType::Scalar)) {
                        ttl = _ttl.as<uint32_t>();

                    } else {
                        fprintf(stderr, "Every event should have a ttl\n");
                        exit(1);
                    }

                    std::string id = _id.as<std::string>();

                    printf("id: %s, group: %s, ttl: %d\n", id.c_str(), group_name.c_str(), ttl);

                    known_event_t* known_event = (known_event_t*)malloc(
                        sizeof(*known_event));

                    known_event->id_len = id.length();
                    known_event->id_len_net = htonl(known_event->id_len);
                    known_event->id_len_size = sizeof(known_event->id_len);

                    char* id_ = (char*)malloc(known_event->id_len + 1);
                    memcpy(id_, id.c_str(), known_event->id_len);
                    id_[known_event->id_len] = '\0';

                    known_event->id = id_;
                    known_event->group = event_group;
                    known_event->ttl = ttl;

                    known_events_t::const_iterator it = known_events.find(id_);

                    if(it == known_events.cend()) {
                        known_events[id_] = known_event;

                    } else {
                        fprintf(stderr, "Duplicate event id: %s\n", id_);
                        exit(1);
                    }

                } else {
                    fprintf(stderr, "Every event should have an id\n");
                    exit(1);
                }
            }
        }
    }

    printf("Running on port: %u\n", my_port);
    signal(SIGPIPE, SIG_IGN);

    if(!db.open(
        db_file_name,
        kyotocabinet::HashDB::OWRITER
        | kyotocabinet::HashDB::OCREATE
        | kyotocabinet::HashDB::ONOLOCK
        | kyotocabinet::HashDB::OAUTOTRAN
    )) {
        printf("Failed to open database: %s\n", db.error().name());
        return 1;
    }

    load_peers_to_discover();

    my_hostname = (char*)"127.0.0.1";
    my_hostname_len = strlen(my_hostname);
    my_peer_id = make_peer_id(my_hostname_len, my_hostname, my_port);
    my_peer_id_len = strlen(my_peer_id);
    my_peer_id_len_net = htonl(my_peer_id_len);
    my_peer_id_len_size = sizeof(my_peer_id_len_net);

    sockaddr_in addr;

    int fh = socket(PF_INET, SOCK_STREAM, 0);

    addr.sin_family = AF_UNSPEC;
    addr.sin_port = htons(my_port);
    addr.sin_addr.s_addr = INADDR_ANY;

    int yes = 1;

    if(setsockopt(fh, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == -1) {
        perror("setsockopt");
        return 1;
    }

    if(bind(fh, (sockaddr*)&addr, sizeof(addr)) != 0) {
        perror("bind");
        return 1;
    }

    fcntl(fh, F_SETFL, fcntl(fh, F_GETFL, 0) | O_NONBLOCK);
    listen(fh, 100000);

    ev_io socket_watcher;
    struct ev_loop* loop = EV_DEFAULT;

    ev_io_init(&socket_watcher, socket_cb, fh, EV_READ);
    ev_io_start(loop, &socket_watcher);

    pthread_mutex_init(&new_clients_mutex, NULL);
    pthread_mutex_init(&known_peers_mutex, NULL);
    pthread_mutex_init(&me_mutex, NULL);
    pthread_mutex_init(&peers_to_discover_mutex, NULL);
    pthread_mutex_init(&stat_mutex, NULL);
    pthread_mutex_init(&replication_exec_queue_mutex, NULL);

    pthread_t synchronization;
    pthread_create(&synchronization, NULL, synchronization_thread, NULL);

    std::queue<pthread_t*> threads;

    for(int i = 0; i < max_client_threads; ++i) {
        pthread_t* thread = (pthread_t*)malloc(sizeof(*thread));

        pthread_create(thread, NULL, client_thread, NULL);

        threads.push(thread);
    }

    {
        peer_to_discover_t* localhost7654 = (peer_to_discover_t*)malloc(
            sizeof(*localhost7654));

        localhost7654->host = "127.0.0.1";
        localhost7654->port = 7654;

        peer_to_discover_t* localhost8765 = (peer_to_discover_t*)malloc(
            sizeof(*localhost8765));

        localhost8765->host = "127.0.0.1";
        localhost8765->port = 8765;

        peers_to_discover[make_peer_id(
            strlen(localhost7654->host),
            (char*)localhost7654->host,
            localhost7654->port

        )] = localhost7654;

        peers_to_discover[make_peer_id(
            strlen(localhost8765->host),
            (char*)localhost8765->host,
            localhost8765->port

        )] = localhost8765;
    }

    pthread_t discovery;
    pthread_create(&discovery, NULL, discovery_thread, NULL);

    pthread_t replication;
    pthread_create(&replication, NULL, replication_thread, NULL);

    pthread_t replication_exec;
    pthread_create(&replication_exec, NULL, replication_exec_thread, NULL);

    ev_run(loop, 0);

    pthread_join(discovery, NULL);

    while(!threads.empty()) {
        pthread_t* thread = threads.front();
        threads.pop();

        pthread_join(*thread, NULL);
        free(thread);
    }

    pthread_join(replication, NULL);
    pthread_join(replication_exec, NULL);
    pthread_join(synchronization, NULL);

    pthread_mutex_destroy(&known_peers_mutex);
    pthread_mutex_destroy(&new_clients_mutex);
    pthread_mutex_destroy(&me_mutex);
    pthread_mutex_destroy(&peers_to_discover_mutex);
    pthread_mutex_destroy(&stat_mutex);
    pthread_mutex_destroy(&replication_exec_queue_mutex);

    return 0;
}

#pragma clang diagnostic pop
