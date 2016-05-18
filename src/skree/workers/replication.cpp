#include "replication.hpp"

namespace Skree {
    namespace Workers {
        void Replication::run() {
            known_peers_t::const_iterator _peer;
            Skree::Client* peer;
            uint64_t now;

            while(true) {
                now = std::time(nullptr);

                for(auto& _event : server.known_events) {
                    // fprintf(stderr, "replication: before read\n");
                    auto event = _event.second;
                    auto queue = event->r_queue;
                    auto item = queue->read();

                    if(item == NULL) {
                        // fprintf(stderr, "replication: empty queue\n");
                        continue;
                    }

                    size_t item_pos = 0;

                    uint32_t rin_len;
                    memcpy(&rin_len, item + item_pos, sizeof(rin_len));
                    item_pos += sizeof(rin_len);
                    rin_len = ntohl(rin_len);

                    // char rin [rin_len]; // TODO: malloc?
                    // memcpy(rin, item + item_pos, rin_len);
                    char* rin = item + item_pos;
                    item_pos += rin_len;

                    uint64_t rts;
                    memcpy(&rts, item + item_pos, sizeof(rts));
                    item_pos += sizeof(rts);
                    rts = ntohll(rts);

                    uint64_t rid_net;
                    memcpy(&rid_net, item + item_pos, sizeof(rid_net));
                    item_pos += sizeof(rid_net);

                    uint64_t rid = ntohll(rid_net);

                    // TODO: is max_id really needed here (or anywhere)?
                    // item_pos += sizeof(uint64_t);

                    uint32_t hostname_len;
                    memcpy(&hostname_len, item + item_pos, sizeof(hostname_len));
                    item_pos += sizeof(hostname_len);
                    hostname_len = ntohl(hostname_len);

                    char* hostname = item + item_pos;
                    item_pos += hostname_len;

                    uint32_t port;
                    memcpy(&port, item + item_pos, sizeof(port));
                    item_pos += sizeof(port);
                    port = htonl(port);

                    uint32_t peers_cnt;
                    memcpy(&peers_cnt, item + item_pos, sizeof(peers_cnt));
                    item_pos += sizeof(peers_cnt);
                    peers_cnt = ntohl(peers_cnt);

                    char* rpr = item + item_pos;

                    char* peer_id = Utils::make_peer_id(hostname_len, hostname, port);
                    uint32_t peer_id_len = strlen(peer_id);

                    // printf("repl thread: %s\n", event->id);

                    size_t suffix_len =
                        event->id_len
                        + 1 // :
                        + peer_id_len
                    ;

                    // TODO
                    char* suffix = (char*)malloc(
                        suffix_len
                        + 1 // :
                        + 20 // wrinseq
                        + 1 // \0
                    );
                    sprintf(suffix, "%s:%s", event->id, peer_id);
                    // printf("repl thread: %s\n", suffix);

                    // TODO
                    uint64_t rinseq;
                    uint64_t wrinseq;

                    suffix[suffix_len] = ':';
                    ++suffix_len;

                    sprintf(suffix + suffix_len, "%lu", wrinseq);
                    suffix_len += 20;
                    size_t suffix_slen = strlen(suffix);

                    // TODO: overflow
                    if((rts + event->ttl) > now) {
                        fprintf(stderr, "skip repl: not now, rts: %lu, now: %lu\n", rts, now);
                        // free(rin);
                        // free(rpr);
                        free(item);
                        free(suffix);
                        queue->sync_read_offset(false);
                        continue;
                    }

                    char* failover_key = suffix;
                    sprintf(failover_key + suffix_len - 20 - 1, "%lu", rid);

                    {
                        failover_t::const_iterator it = server.failover.find(failover_key);

                        if(it != server.failover.cend()) {
                            // TODO: what should really happen here?
                            fprintf(stderr, "skip repl: failover flag is set\n");
                            // free(rin);
                            // free(rpr);
                            free(item);
                            // free(suffix);
                            free(failover_key);
                            queue->sync_read_offset(false);
                            continue;
                        }
                    }

                    {
                        no_failover_t::const_iterator it = server.no_failover.find(failover_key);

                        if(it != server.no_failover.cend()) {
                            if((it->second + server.no_failover_time) > now) {
                                // TODO: what should really happen here?
                                fprintf(stderr, "skip repl: no_failover flag is set\n");
                                // free(rin);
                                // free(rpr);
                                free(item);
                                // free(suffix);
                                free(failover_key);
                                queue->sync_read_offset(false);
                                continue;

                            } else {
                                server.no_failover.erase(it);
                            }
                        }
                    }

                    // TODO: mark task as being processed before
                    //       sync_read_offset() call so it won't be lost
                    queue->sync_read_offset();
                    fprintf(stderr, "replication: after sync_read_offset(), rid: %lu\n", rid);

                    server.failover[failover_key] = 0;

                    pthread_mutex_lock(&(server.known_peers_mutex));

                    _peer = server.known_peers.find(peer_id);

                    if(_peer == server.known_peers.cend()) peer = NULL;
                    else peer = _peer->second;

                    pthread_mutex_unlock(&(server.known_peers_mutex));

                    fprintf(stderr, "Seems like I need to failover task %lu\n", rid);

                    if(peer == NULL) {
                        size_t offset = 0;
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

                        auto data_str = new Utils::muh_str_t {
                            .len = rin_len,
                            .data = rin
                        };

                        auto __peer_id = new Utils::muh_str_t {
                            .len = peer_id_len,
                            .data = peer_id
                        };

                        if(peers_cnt > 0) {
                            *count_replicas = peers_cnt;

                            auto i_req = Skree::Actions::I::out_init(
                                __peer_id, event, rid_net);

                            if(peers_cnt > 0) {
                                have_rpr = true;
                                size_t peer_id_len;
                                char* peer_id;
                                auto _peers_cnt = peers_cnt; // TODO

                                while(peers_cnt > 0) {
                                    peer_id_len = strlen(rpr + offset); // TODO: get rid of this shit
                                    peer_id = (char*)malloc(peer_id_len + 1);
                                    memcpy(peer_id, rpr + offset, peer_id_len);
                                    peer_id[peer_id_len] = '\0';
                                    offset += peer_id_len + 1;

                                    auto it = server.known_peers.find(peer_id);

                                    if(it == server.known_peers.end()) {
                                        ++(*acceptances);

                                    } else {
                                        ++(*pending);

                                        auto ctx = new out_packet_i_ctx {
                                            .count_replicas = count_replicas,
                                            .pending = pending,
                                            .acceptances = acceptances,
                                            .mutex = mutex,
                                            .event = event,
                                            .data = data_str,
                                            .peer_id = __peer_id,
                                            .wrinseq = wrinseq, // TODO
                                            .failover_key = failover_key,
                                            .failover_key_len = suffix_len,
                                            .rpr = rpr,
                                            .peers_cnt = _peers_cnt, // TODO
                                            .rid = rid
                                        };

                                        const auto cb = new Skree::PendingReads::Callbacks::ReplicationProposeSelf(server);
                                        const auto item = new Skree::Base::PendingRead::QueueItem {
                                            .len = 1,
                                            .cb = cb,
                                            .ctx = (void*)ctx,
                                            .opcode = true,
                                            .noop = false
                                        };

                                        auto witem = new Skree::Base::PendingWrite::QueueItem {
                                            .len = i_req->len,
                                            .data = i_req->data,
                                            .pos = 0,
                                            .cb = item
                                        };

                                        it->second->push_write_queue(witem);
                                    }

                                    --peers_cnt;
                                }
                            }
                        }

                        if(!have_rpr) {
                            auto ctx = new out_packet_i_ctx {
                                .count_replicas = count_replicas,
                                .pending = pending,
                                .acceptances = acceptances,
                                .mutex = mutex,
                                .event = event,
                                .data = data_str,
                                .peer_id = __peer_id,
                                .wrinseq = wrinseq, // TODO
                                .failover_key = failover_key,
                                .failover_key_len = suffix_len,
                                .rpr = rpr, // TODO: why is it not NULL here?
                                .peers_cnt = 0,
                                .rid = rid
                            };

                            server.replication_exec(ctx);
                        }

                    } else {
                        // TODO: rin_str's type
                        auto rin_str = new Utils::muh_str_t {
                            .len = rin_len,
                            .data = rin
                        };

                        Utils::muh_str_t* rpr_str = NULL;

                        if(peers_cnt > 0) {
                            rpr_str = new Utils::muh_str_t {
                                .len = (uint32_t)strlen(rpr), // TODO: it is incorrect
                                .data = rpr
                            };
                        }

                        auto ctx = new out_data_c_ctx {
                            .event = event,
                            .rin = rin_str,
                            .rpr = rpr_str, // TODO
                            .rid = rid,
                            .wrinseq = wrinseq, // TODO
                            .failover_key = failover_key,
                            .failover_key_len = suffix_len
                        };

                        const auto cb = new Skree::PendingReads::Callbacks::ReplicationPingTask(server);
                        const auto item = new Skree::Base::PendingRead::QueueItem {
                            .len = 1,
                            .cb = cb,
                            .ctx = (void*)ctx,
                            .opcode = true,
                            .noop = false
                        };

                        auto c_req = Skree::Actions::C::out_init(event, rid_net, rin_len, rin);

                        auto witem = new Skree::Base::PendingWrite::QueueItem {
                            .len = c_req->len,
                            .data = c_req->data,
                            .pos = 0,
                            .cb = item
                        };

                        peer->push_write_queue(witem);
                    }
                }
            }
        }
    }
}
