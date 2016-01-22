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
#define htonll( x ) ( ( 1 == htonl( 1 ) ) ? ( x ) : ( (uint64_t)htonl( \
    ( x ) & 0xFFFFFFFF ) << 32 ) | htonl( ( x ) >> 32 ) )
#endif

#ifndef ntohll
#define ntohll( x ) ( ( 1 == ntohl( 1 ) ) ? ( x ) : ( (uint64_t)ntohl( \
    ( x ) & 0xFFFFFFFF ) << 32 ) | ntohl( ( x ) >> 32 ) )
#endif

#define SAVE_EVENT_RESULT_F 0
#define SAVE_EVENT_RESULT_A 1
#define SAVE_EVENT_RESULT_K 2
#define SAVE_EVENT_RESULT_NULL 3

#define REPL_SAVE_RESULT_F 0
#define REPL_SAVE_RESULT_K 1

static size_t read_size = 131072;
static uint64_t no_failover_time = 10 * 60;
static time_t discovery_timeout_milliseconds = 3000;
static uint32_t max_replication_factor = 3;
static uint32_t max_client_threads = 1;
static uint64_t job_time = 10 * 60;

static uint64_t stat_num_inserts;
static uint64_t stat_num_replications;
static pthread_mutex_t stat_mutex;

static DbWrapper db;

static char* my_hostname;
static uint32_t my_hostname_len;
static uint32_t my_port;
static char* my_peer_id;
static uint32_t my_peer_id_len;
static uint32_t my_peer_id_len_net;
static size_t my_peer_id_len_size;

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

    bool operator()( const char* a, const char* b ) const {

        return ( strcmp( a, b ) == 0 );
    }
};

struct char_pointer_hasher {

    //BKDR hash algorithm
    int operator()( char* key ) const {

        int seed = 131; //31 131 1313 13131131313 etc//
        int hash = 0;
        size_t len = strlen( key );

        for( size_t i = 0; i < len; ++i ) {

            hash = ( ( hash * seed ) + key[ i ] );
        }

        return ( hash & 0x7FFFFFFF );
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
    size_t* out_packet_len;
    char* data;
    char** out_packet;
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

struct in_packet_e_ctx_event {

    uint32_t len;
    char* data;
    char* id;
};

struct in_packet_e_ctx {

    uint32_t cnt;
    uint32_t event_name_len;
    char* event_name;
    std::list<in_packet_e_ctx_event*>* events;
};

struct packet_r_ctx_peer {

    uint32_t hostname_len;
    uint32_t port;
    char* hostname;
};

struct out_packet_r_ctx {

    uint32_t replication_factor;
    uint32_t pending;
    Client* client;
    std::vector<char*>* candidate_peer_ids;
    std::list<packet_r_ctx_peer*>* accepted_peers;
    char* r_req;
    size_t r_len;
    bool sync;
};

struct in_packet_r_ctx_event {

    char* data;
    char* id;
    uint64_t id_net;
    uint32_t len;
};

struct in_packet_r_ctx {

    uint32_t hostname_len;
    uint32_t port;
    char* hostname;
    uint32_t event_name_len;
    uint32_t cnt;
    char* event_name;
    std::list<in_packet_r_ctx_event*>* events;
    std::list<packet_r_ctx_peer*>* peers;
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

struct out_packet_c_ctx {

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

static void client_cb( struct ev_loop* loop, ev_io* _watcher, int events );

static inline char* make_peer_id( size_t peer_name_len, char* peer_name, uint32_t peer_port ) {

    char* peer_id = (char*)malloc( peer_name_len
        + 1 // :
        + 5 // port string
        + 1 // \0
    );

    memcpy( peer_id, peer_name, peer_name_len );
    sprintf( peer_id + peer_name_len, ":%u", peer_port );

    return peer_id;
}

static inline char* get_host_from_sockaddr_in( const sockaddr_in* s_in ) {

    char* conn_name = NULL;

    if( s_in -> sin_family == AF_INET ) {

        conn_name = (char*)malloc( INET_ADDRSTRLEN );
        inet_ntop( AF_INET, &(s_in -> sin_addr), conn_name, INET_ADDRSTRLEN );

    } else {

        conn_name = (char*)malloc( INET6_ADDRSTRLEN );
        inet_ntop( AF_INET6, &(((sockaddr_in6*)s_in) -> sin6_addr), conn_name, INET6_ADDRSTRLEN );
    }

    return conn_name;
}

static inline uint32_t get_port_from_sockaddr_in( const sockaddr_in* s_in ) {

    if( s_in -> sin_family == AF_INET ) {

        return ntohs( s_in -> sin_port );

    } else {

        return ntohs( ((sockaddr_in6*)s_in) -> sin6_port );
    }
}

static inline void save_peers_to_discover() {

    pthread_mutex_lock( &peers_to_discover_mutex );

    size_t cnt = htonll( peers_to_discover.size() );
    size_t dump_len = 0;
    char* dump = (char*)malloc( sizeof( cnt ) );

    memcpy( dump + dump_len, &cnt, sizeof( cnt ) );
    dump_len += sizeof( cnt );

    for(
        peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
        it != peers_to_discover.cend();
        ++it
    ) {

        peer_to_discover_t* peer = it -> second;

        size_t len = strlen( peer -> host );
        uint32_t port = htonl( peer -> port );

        dump = (char*)realloc( dump,
            dump_len
            + sizeof( len )
            + len
            + sizeof( port )
        );

        size_t _len = htonll( len );
        memcpy( dump + dump_len, &_len, sizeof( _len ) );
        dump_len += sizeof( _len );

        memcpy( dump + dump_len, peer -> host, len );
        dump_len += len;

        memcpy( dump + dump_len, &port, sizeof( port ) );
        dump_len += sizeof( port );
    }

    pthread_mutex_unlock( &peers_to_discover_mutex );

    const char* key = "peers_to_discover";
    const size_t key_len = strlen( key );

    if( ! db.set( key, key_len, dump, dump_len ) )
        fprintf( stderr, "Failed to save peers list: %s\n", db.error().name() );
}

static inline void load_peers_to_discover() {

    const char* key = "peers_to_discover";
    const size_t key_len = strlen( key );
    size_t value_len;

    char* value = db.get( key, key_len, &value_len );

    if( value != NULL ) {

        size_t offset = 0;

        size_t cnt;
        memcpy( &cnt, value + offset, sizeof( cnt ) );
        cnt = ntohll( cnt );
        offset += sizeof( cnt );

        while( cnt > 0 ) {

            size_t hostname_len;
            memcpy( &hostname_len, value + offset, sizeof( hostname_len ) );
            hostname_len = ntohll( hostname_len );
            offset += sizeof( hostname_len );

            char* hostname = (char*)malloc( hostname_len + 1 );
            memcpy( hostname, value + offset, hostname_len );
            hostname[ hostname_len ] = '\0';
            offset += hostname_len;

            uint32_t port;
            memcpy( &port, value + offset, sizeof( port ) );
            port = ntohl( port );
            offset += sizeof( port );

            char* peer_id = make_peer_id( hostname_len, hostname, port );

            peers_to_discover_t::const_iterator it = peers_to_discover.find( peer_id );

            if( it == peers_to_discover.cend() ) {

                peer_to_discover_t* peer = (peer_to_discover_t*)malloc( sizeof( *peer ) );

                peer -> host = hostname;
                peer -> port = port;

                peers_to_discover[ peer_id ] = peer;

            } else {

                free( peer_id );
                free( hostname );
            }

            --cnt;
        }
    }
}

static inline void begin_replication( out_packet_r_ctx*& r_ctx );
static inline void continue_replication_exec( out_packet_i_ctx*& ctx );
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

typedef std::unordered_map<char*, muh_str_t*, char_pointer_hasher, char_pointer_comparator> get_keys_result_t;

template <typename T>
static inline T* parse_db_value( muh_str_t* _value, size_t* size = NULL ) {

    if( _value == NULL ) return NULL;
    if( size != NULL ) *size = _value -> len;

    T* value = (T*)malloc( _value -> len );
    memcpy( value, _value -> data, _value -> len );

    // free( _value -> data );
    delete _value;

    return value;
}

template <typename T>
static inline T* parse_db_value( get_keys_result_t* map, std::string* key, size_t* size = NULL ) {

    get_keys_result_t::iterator it = map -> find( (char*)(key -> c_str()) );

    if( it == map -> end() ) return NULL;

    return parse_db_value<T>( it -> second, size );
}

static inline get_keys_result_t* db_get_keys( std::vector<std::string>& keys ) {

    class VisitorImpl : public kyotocabinet::DB::Visitor {

        public:
            explicit VisitorImpl( get_keys_result_t* _out ) : out(_out) {}

        private:
            const char* visit_full(
                const char* _key, size_t key_len,
                const char* _value, size_t value_len,
                size_t* sp
            ) {

                char* key = (char*)malloc( key_len + 1 );
                memcpy( key, _key, key_len );
                key[ key_len ] = '\0';
                // if(strncmp(key,"wrinseq",7)!=0)
                // printf("got %s\n",key);

                char* value = (char*)malloc( value_len );
                memcpy( value, _value, value_len );

                (*out)[ key ] = (muh_str_t*)malloc( sizeof( muh_str_t ) );
                (*out)[ key ] -> len = value_len;
                (*out)[ key ] -> data = value;

                return kyotocabinet::DB::Visitor::NOP;
            }

            get_keys_result_t* out;
    };

    get_keys_result_t* out = new get_keys_result_t();

    VisitorImpl visitor( out );

    if( db.accept_bulk( keys, &visitor, false ) ) return out;
    else return NULL;
}

class Client {

    private:
        struct client_bound_ev_io watcher;
        int fh;
        struct ev_loop* loop;
        char* read_queue;
        size_t read_queue_length;
        size_t read_queue_mapped_length;
        std::queue<WriteQueueItem*> write_queue;
        pthread_mutex_t write_queue_mutex;
        char* peer_name;
        size_t peer_name_len;
        uint32_t peer_port;
        char* conn_name;
        size_t conn_name_len;
        uint32_t conn_port;
        char* peer_id;
        char* conn_id;
        std::deque<PendingReadsQueueItem**> pending_reads;
        sockaddr_in* s_in;
        socklen_t s_in_len;

        void ordinary_packet_cb(
            const char& opcode, char** out_packet,
            size_t* out_packet_len, size_t* in_packet_len
        ) {

            if( opcode == 'w' ) {

                char* _out_packet = (char*)malloc( 1 + sizeof( my_hostname_len )
                    + my_hostname_len );

                _out_packet[ 0 ] = 'k';
                *out_packet_len += 1;

                uint32_t _hostname_len = htonl( my_hostname_len );
                memcpy( _out_packet + *out_packet_len, &_hostname_len,
                        sizeof( _hostname_len ) );
                *out_packet_len += sizeof( _hostname_len );

                memcpy( _out_packet + *out_packet_len, my_hostname, my_hostname_len );
                *out_packet_len += my_hostname_len;

                *out_packet = _out_packet;

            } else if( opcode == 'l' ) {

                uint32_t _known_peers_len;
                char* _out_packet = (char*)malloc( 1 + sizeof( _known_peers_len ) );

                _out_packet[ 0 ] = 'k';
                *out_packet_len += 1;

                pthread_mutex_lock( &known_peers_mutex );

                _known_peers_len = htonl( known_peers.size() );
                memcpy( _out_packet + *out_packet_len, &_known_peers_len,
                    sizeof( _known_peers_len ) );
                *out_packet_len += sizeof( _known_peers_len );

                for(
                    known_peers_t::const_iterator it = known_peers.cbegin();
                    it != known_peers.cend();
                    ++it
                ) {

                    Client* peer = it -> second;

                    uint32_t peer_name_len = peer -> get_peer_name_len();
                    uint32_t _peer_name_len = htonl( peer_name_len );
                    uint32_t _peer_port = htonl( peer -> get_peer_port() );

                    _out_packet = (char*)realloc( _out_packet, *out_packet_len
                        + sizeof( _peer_name_len ) + peer_name_len
                        + sizeof( _peer_port ) );

                    memcpy( _out_packet + *out_packet_len, &_peer_name_len,
                            sizeof( _peer_name_len ) );
                    *out_packet_len += sizeof( _peer_name_len );

                    memcpy( _out_packet + *out_packet_len, peer -> get_peer_name(),
                        peer_name_len );
                    *out_packet_len += peer_name_len;

                    memcpy( _out_packet + *out_packet_len, &_peer_port,
                        sizeof( _peer_port ) );
                    *out_packet_len += sizeof( _peer_port );
                }

                pthread_mutex_unlock( &known_peers_mutex );

                *out_packet = _out_packet;

            } else if( opcode == 'e' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_e_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else if( opcode == 'r' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_r_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else if( opcode == 'c' ) {
// printf("C\n");
                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_c_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else if( opcode == 'i' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_i_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else if( opcode == 'x' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_x_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else if( opcode == 'h' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_h_cb1;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item, true );

            } else {

                printf( "Unknown packet header: 0x%.2X\n", opcode );
            }
        }

        void read_cb() {

            while( read_queue_length > 0 ) {

                size_t in_packet_len = 1;
                size_t out_packet_len = 0;

                char opcode = read_queue[ 0 ];
                char* out_packet = NULL;

                if( ! pending_reads.empty() ) {

                    // Pending reads queue is a top priority callback
                    // If there is a pending read - incoming data should
                    // be passed to such a callback

                    PendingReadsQueueItem** _item = pending_reads.front();
                    PendingReadsQueueItem* item = *_item;

                    if(
                        (
                            ( item -> opcode == true )
                            && (
                                ( opcode == 'k' ) || ( opcode == 'f' )
                                || ( opcode == 'a' )
                            )
                        )
                        || ( item -> opcode == false )
                    ) {
                        // If pending read waits for opcode, and it is the
                        // reply opcode we've got here, or if pending read
                        // does not wait for opcode - process pending read

                        --in_packet_len;

                        if( read_queue_length >= item -> len ) {

                            bool stop = false;

                            PendingReadCallbackArgs args = {
                                .data = read_queue + in_packet_len,
                                .len = item -> len,
                                .out_packet = &out_packet,
                                .out_packet_len = &out_packet_len,
                                .stop = &stop,
                                .ctx = item -> ctx
                            };

                            PendingReadsQueueItem* new_item = (this ->* (item -> cb))( &args );
                            in_packet_len += item -> len;

                            free( item );

                            if( new_item == nullptr ) {

                                // If pending read has received all its data -
                                // remove it from the queue

                                pending_reads.pop_front();
                                free( _item );

                            } else {

                                // If pending read requests more data -
                                // wait for it

                                *_item = new_item;
                            }

                            if( stop ) {

                                delete this;
                                break;
                            }

                        } else {

                            // If we have a pending read, but requested amount
                            // of data is still not arrived - we should wait for it

                            break;
                        }

                    } else {

                        // If pending read waits for opcode, and it is not
                        // the reply opcode we've got here - process data as
                        // ordinary inbound packet

                        // printf("ordinary_packet_cb 1\n");
                        ordinary_packet_cb( opcode, &out_packet, &out_packet_len,
                            &in_packet_len );
                    }

                } else {

                    // There is no pending reads, so data should be processed
                    // as ordinary inbound packet

                    // printf("ordinary_packet_cb 2\n");
                    ordinary_packet_cb( opcode, &out_packet, &out_packet_len,
                        &in_packet_len );
                }

                read_queue_length -= in_packet_len;
                if( read_queue_length > 0 )
                    memmove( read_queue, read_queue + in_packet_len, read_queue_length );

                if( out_packet_len > 0 )
                    push_write_queue( out_packet_len, out_packet, NULL );
            }
        }

        PendingReadsQueueItem* packet_i_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_i_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_i_cb2( PendingReadCallbackArgs* args ) {

            muh_str_t* peer_id = (muh_str_t*)malloc( sizeof( *peer_id ) );

            peer_id -> len = args -> len - 4;
            peer_id -> data = (char*)malloc( peer_id -> len + 1 );

            memcpy( peer_id -> data, args -> data, peer_id -> len );
            peer_id -> data[ peer_id -> len ] = '\0';

            uint32_t _len;
            memcpy( &_len, args -> data + peer_id -> len, 4 );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 8;
            item -> cb = &Client::packet_i_cb3;
            item -> ctx = (void*)peer_id;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_i_cb3( PendingReadCallbackArgs* args ) {

            uint32_t event_id_len = args -> len - 8;
            char* event_id = (char*)malloc( event_id_len + 1 );
            memcpy( event_id, args -> data, event_id_len );
            peer_id[ event_id_len ] = '\0';

            uint64_t _rid;
            memcpy( &_rid, args -> data + event_id_len, 8 );
            uint64_t rid = ntohll( _rid );

            muh_str_t* peer_id = (muh_str_t*)(args -> ctx);

            char* _out_packet = (char*)malloc( 1 );
            *(args -> out_packet_len) += 1;
            *(args -> out_packet) = _out_packet;

            char* suffix = (char*)malloc(
                event_id_len
                + 1 // :
                + peer_id -> len
                + 1 // :
                + 20 // rid
                + 1 // \0
            );
            sprintf( suffix, "%s:%s:%llu", event_id, peer_id -> data, rid );

            failover_t::const_iterator it = failover.find( suffix );

            if( it == failover.cend() ) {

                std::string rre_key( "rre:", 4 );
                rre_key.append( suffix, strlen( suffix ) );

                std::vector<std::string> keys;
                keys.push_back( rre_key );

                get_keys_result_t* dbdata = db_get_keys( keys );

                uint64_t* _rinseq = parse_db_value<uint64_t>( dbdata, &rre_key );

                if( _rinseq == NULL ) {

                    _out_packet[ 0 ] = 'f';

                } else {

                    free( _rinseq );
                    _out_packet[ 0 ] = 'k';
                }

            } else {

                // TODO: It could be 0 here as a special case
                wip_t::const_iterator wip_it = wip.find( it -> second );

                if( wip_it == wip.cend() ) {

                    // TODO: perl
                    // if( int( rand( 100 ) + 0.5 ) > 50 ) {
                    if( false ) {

                        _out_packet[ 0 ] = 'f';

                    } else {

                        _out_packet[ 0 ] = 'k';

                        failover.erase( it );
                        no_failover[ suffix ] = std::time( nullptr );
                    }

                } else {

                    _out_packet[ 0 ] = 'f';
                }
            }

            return nullptr;
        }

        PendingReadsQueueItem* packet_x_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_x_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_x_cb2( PendingReadCallbackArgs* args ) {

            muh_str_t* peer_id = (muh_str_t*)malloc( sizeof( *peer_id ) );

            peer_id -> len = args -> len - 4;
            peer_id -> data = (char*)malloc( peer_id -> len + 1 );

            memcpy( peer_id -> data, args -> data, peer_id -> len );
            peer_id -> data[ peer_id -> len ] = '\0';

            uint32_t _len;
            memcpy( &_len, args -> data + peer_id -> len, 4 );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 8;
            item -> cb = &Client::packet_x_cb3;
            item -> ctx = (void*)peer_id;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_x_cb3( PendingReadCallbackArgs* args ) {

            uint32_t event_id_len = args -> len - 8;
            char* event_id = (char*)malloc( event_id_len + 1 );
            memcpy( event_id, args -> data, event_id_len );
            peer_id[ event_id_len ] = '\0';

            uint64_t _rid;
            memcpy( &_rid, args -> data + event_id_len, 8 );
            uint64_t rid = ntohll( _rid );

            muh_str_t* peer_id = (muh_str_t*)(args -> ctx);

            size_t suffix_len =
                event_id_len
                + 1 // :
                + peer_id -> len
                + 1 // :
                + 20 // rid
            ;
            char* suffix = (char*)malloc(
                suffix_len
                + 1 // \0
            );
            sprintf( suffix, "%s:%s:%llu", event_id, peer_id -> data, rid );

            std::string rre_key( "rre:", 4 );
            rre_key.append( suffix, strlen( suffix ) );

            std::vector<std::string> keys;
            keys.push_back( rre_key );

            get_keys_result_t* dbdata = db_get_keys( keys );

            uint64_t* _rinseq = parse_db_value<uint64_t>( dbdata, &rre_key );

            if( _rinseq != NULL ) {

                repl_clean( suffix_len, suffix, ntohll( *_rinseq ) );
                free( _rinseq );
            }

            delete dbdata;

            return nullptr;
        }

        PendingReadsQueueItem* packet_c_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 8 + 4;
            item -> cb = &Client::packet_c_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_c_cb2( PendingReadCallbackArgs* args ) {

            in_packet_c_ctx* ctx = (in_packet_c_ctx*)malloc( sizeof( *ctx ) );

            ctx -> event_name_len = args -> len - 8 - 4;
            ctx -> event_name = (char*)malloc( ctx -> event_name_len );
            memcpy( ctx -> event_name, args -> data, ctx -> event_name_len );

            uint64_t _rid;
            memcpy( &_rid, args -> data + ctx -> event_name_len, 8 );
            ctx -> rid = ntohll( _rid );

            uint32_t _len;
            memcpy( &_len, args -> data + ctx -> event_name_len + 8, 4 );
            ctx -> rin_len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = ctx -> rin_len;
            item -> cb = &Client::packet_c_cb3;
            item -> ctx = (void*)ctx;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_c_cb3( PendingReadCallbackArgs* args ) {

            in_packet_c_ctx* ctx = (in_packet_c_ctx*)(args -> ctx);

            ctx -> rin = (char*)malloc( args -> len );
            memcpy( ctx -> rin, args -> data, args -> len );

            char* _out_packet = (char*)malloc( 1 );
            *(args -> out_packet_len) += 1;
            *(args -> out_packet) = _out_packet;

            size_t in_key_len = 3;
            char* in_key = (char*)malloc(
                3 // in:
                + ctx -> event_name_len
                + 1 // :
                + 20
                + 1 // \0
            );

            in_key[ 0 ] = 'i';
            in_key[ 1 ] = 'n';
            in_key[ 2 ] = ':';

            memcpy( in_key + in_key_len, ctx -> event_name,
                ctx -> event_name_len );
            in_key_len += ctx -> event_name_len;

            in_key[ in_key_len ] = ':';
            ++in_key_len;

            sprintf( in_key + in_key_len, "%llu", ctx -> rid );

            bool should_save_event = false;

            wip_t::const_iterator it = wip.find( ctx -> rid );

            if( it == wip.cend() ) {

                if( db.check( in_key, in_key_len ) > 0 )
                    should_save_event = true;

                _out_packet[ 0 ] = 'k';

            } else {

                // TODO: check for overflow
                if( ( it -> second + job_time ) <= std::time( nullptr ) ) {

                    should_save_event = true;
                    _out_packet[ 0 ] = 'k';
                    wip.erase( it );

                } else {

                    _out_packet[ 0 ] = 'f';
                }
            }

            if( should_save_event ) {

                in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                    sizeof( *event ) );

                event -> len = ctx -> rin_len;
                event -> data = ctx -> rin;

                in_packet_e_ctx* e_ctx = (in_packet_e_ctx*)malloc( sizeof( *e_ctx ) );

                e_ctx -> cnt = 1;
                e_ctx -> event_name_len = ctx -> event_name_len;
                e_ctx -> event_name = ctx -> event_name;
                e_ctx -> events = new std::list<in_packet_e_ctx_event*>();
                e_ctx -> events -> push_back( event );

                short result = save_event( e_ctx, 0, NULL, NULL );

                if( result != SAVE_EVENT_RESULT_K ) {

                    fprintf( stderr, "save_event() failed: %s\n", db.error().name() );
                    exit( 1 );
                }

                if( ! db.remove( in_key, strlen( in_key ) ) )
                    fprintf( stderr, "db.remove failed: %s\n", db.error().name() );

                Client::free_in_packet_e_ctx( (void*)e_ctx );
            }

            free( in_key );

            return nullptr;
        }

        PendingReadsQueueItem* packet_r_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4 + 4;
            item -> cb = &Client::packet_r_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_r_cb2( PendingReadCallbackArgs* args ) {

            in_packet_r_ctx* ctx = (in_packet_r_ctx*)malloc( sizeof( *ctx ) );

            ctx -> events = new std::list<in_packet_r_ctx_event*>();
            ctx -> peers = new std::list<packet_r_ctx_peer*>();

            uint32_t _port;
            uint32_t _len;
            ctx -> hostname_len = args -> len - sizeof( _port ) - sizeof( _len );

            ctx -> hostname = (char*)malloc( ctx -> hostname_len );
            memcpy( ctx -> hostname, args -> data, ctx -> hostname_len );

            memcpy( &_port, args -> data + ctx -> hostname_len, sizeof( _port ) );
            ctx -> port = ntohl( _port );

            memcpy( &_len, args -> data + ctx -> hostname_len + sizeof( _port ),
                sizeof( _len ) );
            ctx -> event_name_len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = ctx -> event_name_len + 4;
            item -> cb = &Client::packet_r_cb3;
            item -> ctx = (void*)ctx;
            item -> err = &Client::replication_skip_peer;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_r_cb3( PendingReadCallbackArgs* args ) {

            in_packet_r_ctx* ctx = (in_packet_r_ctx*)(args -> ctx);

            ctx -> event_name = (char*)malloc( ctx -> event_name_len );
            memcpy( ctx -> event_name, args -> data, ctx -> event_name_len );

            uint32_t prev_len = args -> len;
            args -> len = prev_len - ctx -> event_name_len;

            char* prev_data = args -> data;
            args -> data = args -> data + ctx -> event_name_len;

            PendingReadsQueueItem* rv = packet_r_cb4( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        PendingReadsQueueItem* packet_r_cb4( PendingReadCallbackArgs* args ) {

            uint32_t _cnt;
            memcpy( &_cnt, args -> data, args -> len );
            uint32_t cnt = ntohl( _cnt );

            if( cnt > 0 ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 8 + 4;
                item -> cb = &Client::packet_r_cb5;
                item -> opcode = false;

                ((in_packet_r_ctx*)(args -> ctx)) -> cnt = htonl( cnt - 1 );

                item -> ctx = args -> ctx;
                item -> err = &Client::replication_skip_peer;

                return item;

            } else {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_r_cb7;
                item -> ctx = args -> ctx;
                item -> err = &Client::replication_skip_peer;
                item -> opcode = false;

                return item;
            }
        }

        PendingReadsQueueItem* packet_r_cb5( PendingReadCallbackArgs* args ) {

            in_packet_r_ctx_event* event = (in_packet_r_ctx_event*)malloc(
                sizeof( *event ) );

            uint64_t _id;
            memcpy( &_id, args -> data, 8 );
            event -> id_net = _id;

            event -> id = (char*)malloc( 21 );
            sprintf( event -> id, "%llu", ntohll( _id ) );
            // printf("repl got id: %llu\n", ntohll(event -> id_net));

            uint32_t _len;
            memcpy( &_len, args -> data + 8, 4 );
            event -> len = ntohl( _len );

            ((in_packet_r_ctx*)(args -> ctx)) -> events -> push_back( event );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = event -> len;
            item -> cb = &Client::packet_r_cb6;
            item -> ctx = args -> ctx;
            item -> err = &Client::replication_skip_peer;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_r_cb6( PendingReadCallbackArgs* args ) {

            uint32_t prev_len = args -> len;
            char* prev_data = args -> data;

            in_packet_r_ctx* ctx = ((in_packet_r_ctx*)(args -> ctx));
            in_packet_r_ctx_event* event = ctx -> events -> back();

            event -> data = (char*)malloc( sizeof( prev_len ) + prev_len );
            memcpy( event -> data, (char*)&prev_len, sizeof( prev_len ) );
            memcpy( event -> data + sizeof( prev_len ), prev_data, prev_len );
            event -> len += sizeof( prev_len );

            args -> len = 4;
            args -> data = (char*)&(ctx -> cnt);

            PendingReadsQueueItem* rv = packet_r_cb4( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        PendingReadsQueueItem* packet_r_cb7( PendingReadCallbackArgs* args ) {

            uint32_t _cnt;
            memcpy( &_cnt, args -> data, args -> len );
            uint32_t cnt = ntohl( _cnt );

            if( cnt > 0 ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_r_cb8;
                item -> opcode = false;

                ((in_packet_r_ctx*)(args -> ctx)) -> cnt = htonl( cnt - 1 );

                item -> ctx = args -> ctx;
                item -> err = &Client::replication_skip_peer;

                return item;

            } else {

                short result = repl_save( (in_packet_r_ctx*)(args -> ctx), this );

                char* _out_packet = (char*)malloc( 1 );
                *(args -> out_packet_len) += 1;
                *(args -> out_packet) = _out_packet;

                if( result == REPL_SAVE_RESULT_F ) {

                    _out_packet[ 0 ] = 'f';

                } else if( result == REPL_SAVE_RESULT_K ) {

                    _out_packet[ 0 ] = 'k';

                } else {

                    fprintf( stderr, "Unexpected repl_save() result: %d\n", result );
                    exit( 1 );
                }

                return nullptr;
            }
        }

        PendingReadsQueueItem* packet_r_cb8( PendingReadCallbackArgs* args ) {

            packet_r_ctx_peer* peer = (packet_r_ctx_peer*)malloc(
                sizeof( *peer ) );

            uint32_t _hostname_len;
            memcpy( &_hostname_len, args -> data, args -> len );
            peer -> hostname_len = ntohl( _hostname_len );

            ((in_packet_r_ctx*)(args -> ctx)) -> peers -> push_back( peer );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = peer -> hostname_len + 4;
            item -> cb = &Client::packet_r_cb9;
            item -> ctx = args -> ctx;
            item -> err = &Client::replication_skip_peer;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_r_cb9( PendingReadCallbackArgs* args ) {

            uint32_t prev_len = args -> len;
            char* prev_data = args -> data;

            in_packet_r_ctx* ctx = ((in_packet_r_ctx*)(args -> ctx));
            packet_r_ctx_peer* peer = ctx -> peers -> back();

            peer -> hostname = (char*)malloc( peer -> hostname_len + 1 );
            memcpy( peer -> hostname, prev_data, peer -> hostname_len );
            peer -> hostname[ peer -> hostname_len ] = '\0';

            uint32_t _port;
            memcpy( &_port, prev_data + peer -> hostname_len, 4 );
            peer -> port = ntohl( _port );

            args -> len = 4;
            args -> data = (char*)&(ctx -> cnt);

            PendingReadsQueueItem* rv = packet_r_cb7( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        PendingReadsQueueItem* packet_e_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_e_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_e_cb2( PendingReadCallbackArgs* args ) {

            uint32_t event_name_len = args -> len - 4;

            char* event_name = (char*)malloc( event_name_len + 1 );
            memcpy( event_name, args -> data, event_name_len );
            event_name[ event_name_len ] = '\0';

            in_packet_e_ctx* ctx = (in_packet_e_ctx*)malloc( sizeof( *ctx ) );

            ctx -> event_name = event_name;
            ctx -> event_name_len = event_name_len;
            ctx -> events = new std::list<in_packet_e_ctx_event*>();

            uint32_t prev_len = args -> len;
            args -> len = prev_len - event_name_len;

            char* prev_data = args -> data;
            args -> data = args -> data + event_name_len;

            args -> ctx = (void*)ctx;

            PendingReadsQueueItem* rv = packet_e_cb3( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        PendingReadsQueueItem* packet_e_cb3( PendingReadCallbackArgs* args ) {

            uint32_t _cnt;
            memcpy( &_cnt, args -> data, args -> len );
            uint32_t cnt = ntohl( _cnt );

            if( cnt > 0 ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_e_cb4;
                item -> opcode = false;

                ((in_packet_e_ctx*)(args -> ctx)) -> cnt = htonl( cnt - 1 );

                item -> ctx = args -> ctx;
                item -> err = &Client::free_in_packet_e_ctx;

                return item;

            } else {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_e_cb6;
                item -> ctx = args -> ctx;
                item -> err = &Client::free_in_packet_e_ctx;
                item -> opcode = false;

                return item;
            }
        }

        PendingReadsQueueItem* packet_e_cb4( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len;
            item -> cb = &Client::packet_e_cb5;
            item -> ctx = args -> ctx;
            item -> err = &Client::free_in_packet_e_ctx;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_e_cb5( PendingReadCallbackArgs* args ) {

            uint32_t prev_len = args -> len;
            char* prev_data = args -> data;

            in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                sizeof( *event ) );

            event -> len = prev_len;
            event -> data = (char*)malloc( prev_len );
            event -> id = NULL;

            memcpy( event -> data, prev_data, prev_len );

            in_packet_e_ctx* ctx = ((in_packet_e_ctx*)(args -> ctx));

            ctx -> events -> push_back( event );

            args -> len = 4;
            args -> data = (char*)&(ctx -> cnt);

            PendingReadsQueueItem* rv = packet_e_cb3( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        PendingReadsQueueItem* packet_e_cb6( PendingReadCallbackArgs* args ) {

            in_packet_e_ctx* ctx = (in_packet_e_ctx*)(args -> ctx);

            known_events_t::const_iterator it = known_events.find( ctx -> event_name );

            if( it == known_events.cend() ) {

                fprintf( stderr, "Got unknown event: %s\n", ctx -> event_name );
                Client::free_in_packet_e_ctx( (void*)ctx );

                return nullptr;
            }

            uint32_t _replication_factor;
            memcpy( &_replication_factor, args -> data, args -> len );
            uint32_t replication_factor = ntohl( _replication_factor );

            short result = save_event( ctx, replication_factor, this, NULL );

            char* _out_packet = (char*)malloc( 1 );
            *(args -> out_packet_len) += 1;
            *(args -> out_packet) = _out_packet;

            switch( result ) {

                case SAVE_EVENT_RESULT_F:
                    _out_packet[ 0 ] = 'f';
                    break;
                case SAVE_EVENT_RESULT_A:
                    _out_packet[ 0 ] = 'a';
                    break;
                case SAVE_EVENT_RESULT_K:
                    _out_packet[ 0 ] = 'k';
                    break;
                case SAVE_EVENT_RESULT_NULL:
                    free( _out_packet );
                    *(args -> out_packet) = NULL;
                    *(args -> out_packet_len) = 0;
                    break;
                default:
                    fprintf( stderr, "Unexpected save_event() result: %d\n", result );
                    exit( 1 );
            };

            Client::free_in_packet_e_ctx( (void*)ctx );

            return nullptr;
        }

        PendingReadsQueueItem* packet_h_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_h_cb2;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_h_cb2( PendingReadCallbackArgs* args ) {

            uint32_t host_len = args -> len - 4;

            char host[ host_len ];
            memcpy( host, args -> data, host_len );

            uint32_t _port;
            memcpy( &_port, args -> data + host_len, 4 );
            uint32_t port = ntohl( _port );

            char* out_packet = (char*)malloc( 1 );
            *(args -> out_packet) = out_packet;
            *(args -> out_packet_len) = 1;

            char* _peer_id = make_peer_id( host_len, host, port );

            pthread_mutex_lock( &known_peers_mutex );

            known_peers_t::const_iterator known_peer = known_peers.find( _peer_id );

            if( known_peer == known_peers.cend() ) {

                out_packet[ 0 ] = 'k';

                peer_name = (char*)malloc( host_len );
                memcpy( peer_name, host, host_len );

                peer_name_len = host_len;
                peer_port = port;
                peer_id = _peer_id;

                known_peers[ _peer_id ] = this;
                known_peers_by_conn_id[ get_conn_id() ] = this;

            } else {

                free( _peer_id );
                out_packet[ 0 ] = 'f';
            }

            pthread_mutex_unlock( &known_peers_mutex );

            return nullptr;
        }

        PendingReadsQueueItem* discovery_cb9( PendingReadCallbackArgs* args ) {

            uint32_t host_len = args -> len - 4;

            char* host = (char*)malloc( host_len + 1 );
            memcpy( host, args -> data, host_len );
            host[ host_len ] = '\0';

            uint32_t _port;
            memcpy( &_port, args -> data + host_len, 4 );
            uint32_t port = ntohl( _port );

            char* _peer_id = make_peer_id( host_len, host, port );

            pthread_mutex_lock( &peers_to_discover_mutex );

            peers_to_discover_t::const_iterator prev_item = peers_to_discover.find( _peer_id );

            if( prev_item == peers_to_discover.cend() ) {

                peer_to_discover_t* peer_to_discover = (peer_to_discover_t*)malloc(
                    sizeof( *peer_to_discover ) );

                peer_to_discover -> host = host;
                peer_to_discover -> port = port;

                peers_to_discover[ _peer_id ] = peer_to_discover;
                save_peers_to_discover();

            } else {

                free( _peer_id );
                free( host );
            }

            pthread_mutex_unlock( &peers_to_discover_mutex );

            uint32_t prev_len = args -> len;
            args -> len = 4;

            char* prev_data = args -> data;
            args -> data = (char*)(args -> ctx);

            PendingReadsQueueItem* rv = discovery_cb7( args );

            args -> len = prev_len;
            args -> data = prev_data;

            return rv;
        }

        static void free_discovery_ctx( void* _ctx ) {

            uint32_t* ctx = (uint32_t*)_ctx;

            free( ctx );
        }

        PendingReadsQueueItem* discovery_cb8( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::discovery_cb9;
            item -> ctx = args -> ctx;
            item -> err = &Client::free_discovery_ctx;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* discovery_cb7( PendingReadCallbackArgs* args ) {

            uint32_t _cnt;
            memcpy( &_cnt, args -> data, args -> len );
            uint32_t cnt = ntohl( _cnt );

            if( cnt > 0 ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::discovery_cb8;
                item -> opcode = false;

                _cnt = htonl( cnt - 1 );

                if( args -> ctx == NULL ) {

                    uint32_t* ctx = (uint32_t*)malloc( sizeof( *ctx ) );
                    memcpy( ctx, &_cnt, args -> len );

                    item -> ctx = (void*)ctx;
                    item -> err = &Client::free_discovery_ctx;

                } else {

                    memcpy( args -> ctx, &_cnt, args -> len );

                    item -> ctx = args -> ctx;
                    item -> err = &Client::free_discovery_ctx;
                }

                return item;

            } else {

                if( args -> ctx != NULL )
                    free( args -> ctx );

                return nullptr;
            }
        }

        PendingReadsQueueItem* discovery_cb6( PendingReadCallbackArgs* args ) {

            if( args -> data[ 0 ] == 'k' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::discovery_cb7;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                return item;

            } else {

                return nullptr;
            }
        }

        PendingReadsQueueItem* discovery_cb5( PendingReadCallbackArgs* args ) {

            if( args -> data[ 0 ] == 'k' ) {

                pthread_mutex_lock( &known_peers_mutex );

                // peer_id is guaranteed to be set here
                known_peers_t::const_iterator known_peer = known_peers.find( peer_id );

                if( known_peer == known_peers.cend() ) {

                    known_peers[ peer_id ] = this;
                    known_peers_by_conn_id[ get_conn_id() ] = this;

                } else {

                    *(args -> stop) = true;
                }

                pthread_mutex_unlock( &known_peers_mutex );

                if( ! *(args -> stop) ) {

                    char* l_req = (char*)malloc( 1 );
                    l_req[ 0 ] = 'l';

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                        sizeof( *item ) );

                    item -> len = 1;
                    item -> cb = &Client::discovery_cb6;
                    item -> ctx = NULL;
                    item -> err = NULL;
                    item -> opcode = true;

                    push_write_queue( 1, l_req, item );
                }

                return nullptr;

            } else {

                *(args -> stop) = true;
            }

            return nullptr;
        }

        PendingReadsQueueItem* discovery_cb4( PendingReadCallbackArgs* args ) {

            uint32_t port = get_conn_port();
            char* _peer_id = make_peer_id( args -> len, args -> data, port );
            bool accepted = false;

            pthread_mutex_lock( &known_peers_mutex );

            known_peers_t::const_iterator known_peer = known_peers.find( _peer_id );

            if( known_peer == known_peers.cend() ) {

                if( strcmp( _peer_id, my_peer_id ) == 0 ) {

                    pthread_mutex_lock( &me_mutex );

                    me_t::const_iterator it = me.find( _peer_id );

                    if( it == me.cend() ) {

                        char* _conn_peer_id = get_conn_id();

                        me[ _peer_id ] = true;
                        me[ strdup( _conn_peer_id ) ] = true;

                    } else {

                        free( _peer_id );
                    }

                    pthread_mutex_unlock( &me_mutex );

                } else {

                    peer_name = (char*)malloc( args -> len );
                    memcpy( peer_name, args -> data, args -> len );

                    peer_name_len = args -> len;
                    peer_port = port;
                    peer_id = _peer_id;

                    accepted = true;
                }

            } else {

                free( _peer_id );
            }

            pthread_mutex_unlock( &known_peers_mutex );

            if( accepted ) {

                size_t h_len = 0;
                char* h_req = (char*)malloc( 1
                    + sizeof( my_hostname_len )
                    + my_hostname_len
                    + sizeof( my_port )
                );

                h_req[ 0 ] = 'h';
                h_len += 1;

                uint32_t _hostname_len = htonl( my_hostname_len );
                memcpy( h_req + h_len, &_hostname_len, sizeof( _hostname_len ) );
                h_len += sizeof( _hostname_len );

                memcpy( h_req + h_len, my_hostname, my_hostname_len );
                h_len += my_hostname_len;

                uint32_t _my_port = htonl( my_port );
                memcpy( h_req + h_len, &_my_port, sizeof( _my_port ) );
                h_len += sizeof( _my_port );

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 1;
                item -> cb = &Client::discovery_cb5;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = true;

                push_write_queue( h_len, h_req, item );

            } else {

                *(args -> stop) = true;
            }

            return nullptr;
        }

        PendingReadsQueueItem* discovery_cb3( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len;
            item -> cb = &Client::discovery_cb4;
            item -> ctx = NULL;
            item -> err = NULL;
            item -> opcode = false;

            return item;
        }

    public:
        Client( int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len )
            : fh( _fh ), loop( _loop ), s_in( _s_in ), s_in_len( _s_in_len ) {

            read_queue = NULL;
            peer_name = NULL;
            peer_id = NULL;
            conn_name = NULL;
            conn_id = NULL;
            conn_port = 0;
            read_queue_length = 0;
            read_queue_mapped_length = 0;
            pthread_mutex_init( &write_queue_mutex, NULL );

            fcntl( fh, F_SETFL, fcntl( fh, F_GETFL, 0 ) | O_NONBLOCK );

            watcher.client = this;
            ev_io_init( &watcher.watcher, client_cb, fh, EV_READ | EV_WRITE );
            ev_io_start( loop, &watcher.watcher );
        }

        ~Client() {

            pthread_mutex_lock( &known_peers_mutex );

            ev_io_stop( loop, &watcher.watcher );
            shutdown( fh, SHUT_RDWR );
            close( fh );
            free( s_in );

            if( peer_id != NULL ) {

                known_peers_t::const_iterator known_peer = known_peers.find( peer_id );

                if( known_peer != known_peers.cend() )
                    known_peers.erase( known_peer );

                free( peer_id );
            }

            if( conn_id != NULL ) {

                known_peers_t::const_iterator known_peer = known_peers_by_conn_id.find( conn_id );

                if( known_peer != known_peers_by_conn_id.cend() )
                    known_peers_by_conn_id.erase( known_peer );

                free( conn_id );
            }

            pthread_mutex_unlock( &known_peers_mutex );

            while( ! pending_reads.empty() ) {

                PendingReadsQueueItem** _item = pending_reads.front();
                PendingReadsQueueItem* item = *_item;

                if( item -> ctx != NULL ) {

                    if( item -> err == NULL ) {

                        fprintf( stderr, "Don't known how to free pending read context\n" );

                    } else {

                        item -> err( item -> ctx );
                    }
                }

                free( item );
                free( _item );

                pending_reads.pop_front();
            }

            while( ! write_queue.empty() ) {

                WriteQueueItem* item = write_queue.front();
                write_queue.pop();

                if( item -> cb != NULL ) {

                    if( item -> cb -> ctx != NULL ) {

                        if( item -> cb -> err == NULL ) {

                            fprintf( stderr, "Don't known how to free pending read context\n" );

                        } else {

                            item -> cb -> err( item -> cb -> ctx );
                        }
                    }

                    free( item -> cb );
                }

                free( item -> data );
                free( item );
            }

            pthread_mutex_destroy( &write_queue_mutex );

            if( read_queue != NULL ) free( read_queue );
            if( peer_name != NULL ) free( peer_name );
            if( conn_name != NULL ) free( conn_name );
        }

        char* get_peer_name() {

            return peer_name;
        }

        uint32_t get_peer_name_len() {

            return peer_name_len;
        }

        uint32_t get_peer_port() {

            return peer_port;
        }

        uint32_t get_conn_port() {

            if( conn_port > 0 )
                return conn_port;

            conn_port = get_port_from_sockaddr_in( s_in );

            return conn_port;
        }

        char* get_conn_name() {

            if( conn_name != NULL )
                return conn_name;

            conn_name = get_host_from_sockaddr_in( s_in );
            conn_name_len = strlen( conn_name );

            return conn_name;
        }

        uint32_t get_conn_name_len() {

            if( conn_name == NULL ) get_conn_name();

            return conn_name_len;
        }

        char* get_peer_id() {

            if( peer_id != NULL )
                return peer_id;

            if( peer_name == NULL )
                return NULL;

            peer_id = make_peer_id( peer_name_len, peer_name, peer_port );

            return peer_id;
        }

        char* get_conn_id() {

            if( conn_id != NULL )
                return conn_id;

            char* _conn_name = get_conn_name();

            conn_id = make_peer_id( conn_name_len, _conn_name, get_conn_port() );

            return conn_id;
        }

        void push_read_queue( size_t len, char* data ) {

            size_t offset = read_queue_length;

            if( read_queue == NULL ) {

                read_queue_length = len;
                read_queue_mapped_length = read_size +
                    ( ( read_queue_length > read_size ) ? read_queue_length : 0 );

                read_queue = (char*)malloc( read_queue_mapped_length );

            } else {

                read_queue_length += len;

                if( read_queue_length > read_queue_mapped_length ) {

                    read_queue_mapped_length = read_size + read_queue_length;
                    read_queue = (char*)realloc( read_queue, read_queue_length );
                }
            }

            memcpy( read_queue + offset, data, len );
            read_cb();

            return;
        }

        WriteQueueItem* get_pending_write() {

            WriteQueueItem* result = NULL;

            pthread_mutex_lock( &write_queue_mutex );

            while( ! write_queue.empty() ) {

                WriteQueueItem* item = write_queue.front();

                if( ( item -> len > 0 ) && ( item -> pos < item -> len ) ) {

                    result = item;
                    break;

                } else {

                    write_queue.pop();
                    free( item -> data );
                    if( item -> cb != NULL ) free( item -> cb );
                    free( item );
                }
            }

            if( result == NULL )
                ev_io_set( &watcher.watcher, fh, EV_READ );

            pthread_mutex_unlock( &write_queue_mutex );

            return result;
        }

        void push_write_queue( size_t len, char* data, PendingReadsQueueItem* cb ) {

            WriteQueueItem* item = (WriteQueueItem*)malloc( sizeof( *item ) );

            item -> len = len;
            item -> data = data;
            item -> pos = 0;
            item -> cb = cb;

            pthread_mutex_lock( &write_queue_mutex );

            if( write_queue.empty() )
                ev_io_set( &watcher.watcher, fh, EV_READ | EV_WRITE );

            write_queue.push( item );

            pthread_mutex_unlock( &write_queue_mutex );
        }

        void push_pending_reads_queue( PendingReadsQueueItem* item, bool front = false ) {

            PendingReadsQueueItem** _item = (PendingReadsQueueItem**)malloc(
                sizeof( *_item ) );
            *_item = item;

            if(front)
                pending_reads.push_front( _item );
            else
                pending_reads.push_back( _item );
        }

        PendingReadsQueueItem* discovery_cb2( PendingReadCallbackArgs* args ) {

            if( args -> data[ 0 ] == 'k' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::discovery_cb3;
                item -> ctx = NULL;
                item -> err = NULL;
                item -> opcode = false;

                return item;

            } else {

                *(args -> stop) = true;

                return nullptr;
            }
        }

        PendingReadsQueueItem* replication_cb( PendingReadCallbackArgs* args ) {

            out_packet_r_ctx* ctx = (out_packet_r_ctx*)(args -> ctx);
            --(ctx -> pending);

            if( args -> data[ 0 ] == 'k' ) {

                packet_r_ctx_peer* peer =
                    (packet_r_ctx_peer*)malloc( sizeof( *peer ) );

                peer -> hostname_len = get_peer_name_len();
                peer -> hostname = get_peer_name();
                peer -> port = htonl( get_peer_port() );

                ctx -> accepted_peers -> push_back( peer );
            }

            begin_replication( ctx );

            return nullptr;
        }

        static void replication_skip_peer( void* _ctx ) {

            out_packet_r_ctx* ctx = (out_packet_r_ctx*)_ctx;
            --(ctx -> pending);

            begin_replication( ctx );
        }

        PendingReadsQueueItem* propose_self_k_cb( PendingReadCallbackArgs* args ) {

            out_packet_i_ctx* ctx = (out_packet_i_ctx*)(args -> ctx);

            pthread_mutex_lock( ctx -> mutex );

            --(*(ctx -> pending));

            if( args -> data[ 0 ] == 'k' )
                ++(*(ctx -> acceptances));

            continue_replication_exec( ctx );

            pthread_mutex_unlock( ctx -> mutex );

            return nullptr;
        }

        static void propose_self_f_cb( void* _ctx ) {

            out_packet_i_ctx* ctx = (out_packet_i_ctx*)_ctx;

            pthread_mutex_lock( ctx -> mutex );

            --(*(ctx -> pending));

            continue_replication_exec( ctx );

            pthread_mutex_unlock( ctx -> mutex );
        }

        static void free_in_packet_e_ctx( void* _ctx ) {

            in_packet_e_ctx* ctx = (in_packet_e_ctx*)_ctx;

            for(
                std::list<in_packet_e_ctx_event*>::const_iterator it = ctx -> events -> cbegin();
                it != ctx -> events -> cend();
                ++it
            ) {

                in_packet_e_ctx_event* event = *it;

                free( event -> data );
                if( event -> id != NULL ) free( event -> id );
                free( event );
            }

            free( ctx -> event_name );
            free( ctx -> events );
            free( ctx );
        }

        PendingReadsQueueItem* ping_task_k_cb( PendingReadCallbackArgs* args ) {

            out_packet_c_ctx* ctx = (out_packet_c_ctx*)(args -> ctx);

            if( args -> data[ 0 ] == 'k' ) {

                repl_clean(
                    ctx -> failover_key_len,
                    ctx -> failover_key,
                    ctx -> wrinseq
                );

            } else {

                ping_task_f_cb( (void*)ctx );
            }

            // TODO: perl
            // $unfailover -> ();

            // pthread_mutex_lock( ctx -> mutex );
            //
            // pthread_mutex_unlock( ctx -> mutex );

            return nullptr;
        }

        static void ping_task_f_cb( void* _ctx ) {

            out_packet_c_ctx* ctx = (out_packet_c_ctx*)_ctx;
            in_packet_r_ctx* r_ctx = (in_packet_r_ctx*)malloc( sizeof( *r_ctx ) );

            r_ctx -> hostname_len = ctx -> client -> get_peer_name_len();
            r_ctx -> port = ctx -> client -> get_peer_port();
            r_ctx -> hostname = strndup(
                ctx -> client -> get_peer_name(),
                r_ctx -> hostname_len
            );
            r_ctx -> event_name_len = ctx -> event -> id_len;
            r_ctx -> event_name = strndup(
                ctx -> event -> id,
                r_ctx -> event_name_len
            );
            r_ctx -> events = new std::list<in_packet_r_ctx_event*>();
            r_ctx -> cnt = 0;
            r_ctx -> peers = new std::list<packet_r_ctx_peer*>();

            in_packet_r_ctx_event* event = (in_packet_r_ctx_event*)malloc(
                sizeof( *event ) );

            event -> len = ctx -> rin -> len;
            event -> data = ctx -> rin -> data;
            event -> id = (char*)malloc( 21 );
            sprintf( event -> id, "%llu", ctx -> rid );

            r_ctx -> events -> push_back( event );

            if( ctx -> rpr != NULL ) {

                size_t rpr_len = ctx -> rpr -> len;
                size_t rpr_offset = 0;

                while( rpr_offset < rpr_len ) {

                    size_t peer_id_len = strlen( ctx -> rpr -> data + rpr_offset );
                    char* peer_id = (char*)malloc( peer_id_len + 1 );
                    memcpy( peer_id, ctx -> rpr -> data + rpr_offset, peer_id_len );
                    peer_id[ peer_id_len ] = '\0';
                    rpr_offset += peer_id_len + 1;

                    char* delimiter = rindex( peer_id, ':' );

                    if( delimiter == NULL ) {

                        fprintf( stderr, "Invalid peer id: %s\n", peer_id );

                    } else {

                        packet_r_ctx_peer* peer = (packet_r_ctx_peer*)malloc(
                            sizeof( *peer ) );

                        peer -> hostname_len = delimiter - peer_id;
                        peer -> port = atoi( delimiter + 1 );
                        peer -> hostname = (char*)malloc( peer -> hostname_len + 1 );

                        memcpy( peer -> hostname, peer_id, peer -> hostname_len );
                        peer -> hostname[ peer -> hostname_len ] = '\0';

                        r_ctx -> peers -> push_back( peer );
                    }
                }
            }

            short result = repl_save( r_ctx, ctx -> client );

            if( result == REPL_SAVE_RESULT_K ) {

                repl_clean(
                    ctx -> failover_key_len,
                    ctx -> failover_key,
                    ctx -> wrinseq
                );

            } else if( result == REPL_SAVE_RESULT_F ) {

                fprintf( stderr, "repl_save() failed\n" );
                exit( 1 );

            } else {

                fprintf( stderr, "Unexpected repl_save() result: %d\n", result );
                exit( 1 );
            }

            // TODO: perl
            // $unfailover -> ();

            // pthread_mutex_lock( ctx -> mutex );
            //
            // pthread_mutex_unlock( ctx -> mutex );
        }
};

static inline short save_event(
    in_packet_e_ctx* ctx,
    uint32_t replication_factor,
    Client* client,
    std::vector<uint64_t>* task_ids
) {

    short result = SAVE_EVENT_RESULT_F;

    if( replication_factor > max_replication_factor )
        replication_factor = max_replication_factor;

    uint64_t increment_key_len = 6 + ctx -> event_name_len;
    char* increment_key = (char*)malloc( increment_key_len );

    sprintf( increment_key, "inseq:" );
    memcpy( increment_key + 6, ctx -> event_name, ctx -> event_name_len );

    uint32_t _cnt = htonl( ctx -> events -> size() );
    size_t r_len = 0;
    char* r_req = (char*)malloc( 1
        + sizeof( my_hostname_len )
        + my_hostname_len
        + sizeof( my_port )
        + sizeof( ctx -> event_name_len )
        + ctx -> event_name_len
        + sizeof( _cnt )
    );

    r_req[ 0 ] = 'r';
    r_len += 1;

    uint32_t _hostname_len = htonl( my_hostname_len );
    memcpy( r_req + r_len, &_hostname_len, sizeof( _hostname_len ) );
    r_len += sizeof( _hostname_len );

    memcpy( r_req + r_len, my_hostname, my_hostname_len );
    r_len += my_hostname_len;

    uint32_t _my_port = htonl( my_port );
    memcpy( r_req + r_len, (char*)&_my_port, sizeof( _my_port ) );
    r_len += sizeof( _my_port );

    uint32_t _event_name_len = htonl( ctx -> event_name_len );
    memcpy( r_req + r_len, (char*)&_event_name_len, sizeof( _event_name_len ) );
    r_len += sizeof( _event_name_len );

    memcpy( r_req + r_len, ctx -> event_name, ctx -> event_name_len );
    r_len += ctx -> event_name_len;

    memcpy( r_req + r_len, (char*)&_cnt, sizeof( _cnt ) );
    r_len += sizeof( _cnt );

    uint32_t num_inserted = 0;
    bool replication_began = false;

    if( db.begin_transaction() ) {

        int64_t max_id = db.increment(
            increment_key,
            increment_key_len,
            ctx -> events -> size(),
            0
        );

        if( max_id == kyotocabinet::INT64MIN ) {

            fprintf( stderr, "Increment failed: %s\n", db.error().name() );

            if( ! db.end_transaction( false ) ) {

                fprintf( stderr, "Failed to abort transaction: %s\n", db.error().name() );
                exit( 1 );
            }

        } else {

            for(
                std::list<in_packet_e_ctx_event*>::const_iterator it =
                    ctx -> events -> cbegin();
                it != ctx -> events -> cend();
                ++it
            ) {

                in_packet_e_ctx_event* event = *it;

                event -> id = (char*)malloc( 21 );
                sprintf( event -> id, "%llu", max_id );

                if( task_ids != NULL )
                    task_ids -> push_back( max_id );

                uint32_t key_len =
                    3 // in:
                    + ctx -> event_name_len
                    + 1 // :
                    + strlen( event -> id )
                ;
                char* key = (char*)malloc( key_len );

                sprintf( key, "in:" );
                memcpy( key + 3, ctx -> event_name, ctx -> event_name_len );
                key[ 3 + ctx -> event_name_len ] = ':';
                memcpy( key + 3 + ctx -> event_name_len + 1, event -> id,
                    strlen( event -> id ) );

                if( db.add( key, key_len, event -> data, event -> len ) ) {

                    free( key );
                    ++num_inserted;

                } else {

                    fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );
                    free( key );
                    break;
                }

                uint64_t _max_id = htonll( max_id );

                r_req = (char*)realloc( r_req,
                    r_len
                    + sizeof( _max_id )
                    + sizeof( event -> len )
                    + event -> len
                );

                memcpy( r_req + r_len, (char*)&_max_id, sizeof( _max_id ) );
                r_len += sizeof( _max_id );

                uint32_t _event_len = htonl( event -> len );
                memcpy( r_req + r_len, (char*)&_event_len, sizeof( _event_len ) );
                r_len += sizeof( _event_len );

                memcpy( r_req + r_len, event -> data, event -> len );
                r_len += event -> len;

                --max_id;
            }

            if( num_inserted == ctx -> events -> size() ) {

                if( db.end_transaction( true ) ) {

                    pthread_mutex_lock( &stat_mutex );
                    stat_num_inserts += num_inserted;
                    pthread_mutex_unlock( &stat_mutex );

                    if( replication_factor > 0 ) {

                        out_packet_r_ctx* r_ctx = (out_packet_r_ctx*)
                            malloc( sizeof( *r_ctx ) );

                        r_ctx -> sync = true;
                        r_ctx -> replication_factor = replication_factor;
                        r_ctx -> pending = 0;
                        r_ctx -> client = client;
                        r_ctx -> candidate_peer_ids = new std::vector<char*>();
                        r_ctx -> accepted_peers = new std::list<
                            packet_r_ctx_peer*>();

                        pthread_mutex_lock( &known_peers_mutex );

                        for(
                            known_peers_t::const_iterator it = known_peers.cbegin();
                            it != known_peers.cend();
                            ++it
                        ) {

                            r_ctx -> candidate_peer_ids -> push_back( it -> first );
                        }

                        pthread_mutex_unlock( &known_peers_mutex );

                        if( r_ctx -> candidate_peer_ids -> size() > 0 ) {

                            result = SAVE_EVENT_RESULT_NULL;

                            std::random_shuffle(
                                r_ctx -> candidate_peer_ids -> begin(),
                                r_ctx -> candidate_peer_ids -> end()
                            );

                            r_ctx -> r_req = r_req;
                            r_ctx -> r_len = r_len;

                            begin_replication( r_ctx );
                            replication_began = true;

                        } else {

                            result = SAVE_EVENT_RESULT_A;
                        }

                    } else {

                        result = SAVE_EVENT_RESULT_K;
                    }

                } else {

                    fprintf( stderr, "Failed to commit transaction: %s\n",
                        db.error().name() );
                    exit( 1 );
                }

            } else {

                fprintf( stderr, "Batch insert failed\n" );

                if( ! db.end_transaction( false ) ) {

                    fprintf( stderr, "Failed to abort transaction: %s\n",
                        db.error().name() );
                    exit( 1 );
                }
            }
        }

    } else {

        fprintf( stderr, "Failed to start transaction: %s\n", db.error().name() );
        exit( 1 );
    }

    free( increment_key );
    if( ! replication_began ) free( r_req );

    return result;
}

static inline short repl_save(
    in_packet_r_ctx* ctx,
    Client* client
) {

    short result = REPL_SAVE_RESULT_F;

    char* _peer_id = client -> get_peer_id();
    size_t _peer_id_len = strlen( _peer_id );

    uint64_t increment_key_len = 7 + ctx -> event_name_len;
    char* increment_key = (char*)malloc( increment_key_len + 1 + _peer_id_len );

    sprintf( increment_key, "rinseq:" );
    memcpy( increment_key + 7, ctx -> event_name, ctx -> event_name_len );
    increment_key[ increment_key_len ] = ':';
    ++increment_key_len;
    memcpy( increment_key + increment_key_len, _peer_id, _peer_id_len );
    increment_key_len += _peer_id_len;

    uint32_t num_inserted = 0;

    uint32_t peers_cnt = ctx -> peers -> size();
    uint32_t _peers_cnt = htonl( peers_cnt );
    uint64_t serialized_peers_len = sizeof( _peers_cnt );
    char* serialized_peers = (char*)malloc( serialized_peers_len );
    memcpy( serialized_peers, &_peers_cnt, serialized_peers_len );

    for(
        std::list<packet_r_ctx_peer*>::const_iterator it =
            ctx -> peers -> cbegin();
        it != ctx -> peers -> cend();
        ++it
    ) {

        packet_r_ctx_peer* peer = *it;
        char* _peer_id = make_peer_id( peer -> hostname_len,
            peer -> hostname, peer -> port );

        bool keep_peer_id = false;
        size_t _peer_id_len = strlen( _peer_id );

        pthread_mutex_lock( &peers_to_discover_mutex );

        peers_to_discover_t::const_iterator prev_item =
            peers_to_discover.find( _peer_id );

        if( prev_item == peers_to_discover.cend() ) {

            peer_to_discover_t* peer_to_discover = (peer_to_discover_t*)malloc(
                sizeof( *peer_to_discover ) );

            peer_to_discover -> host = peer -> hostname;
            peer_to_discover -> port = peer -> port;

            peers_to_discover[ _peer_id ] = peer_to_discover;
            keep_peer_id = true;
            save_peers_to_discover();
        }

        pthread_mutex_unlock( &peers_to_discover_mutex );

        serialized_peers = (char*)realloc( serialized_peers,
            serialized_peers_len + _peer_id_len );

        memcpy( serialized_peers + serialized_peers_len,
            _peer_id, _peer_id_len );
        serialized_peers_len += _peer_id_len;

        if( ! keep_peer_id ) free( _peer_id );
    }
// printf("repl_save: before begin_transaction\n");
    if( db.begin_transaction() ) {
// printf("repl_save: after begin_transaction\n");
        int64_t max_id = db.increment(
            increment_key,
            increment_key_len,
            ctx -> events -> size(),
            0
        );

        if( max_id == kyotocabinet::INT64MIN ) {

            if( ! db.end_transaction( false ) ) {

                fprintf( stderr, "Failed to abort transaction: %s\n", db.error().name() );
                exit( 1 );
            }

        } else {

            uint64_t _now = htonll( std::time( nullptr ) );
            size_t now_len = sizeof( _now );
            char* now = (char*)malloc( now_len );
            memcpy( now, &_now, now_len );

            for(
                std::list<in_packet_r_ctx_event*>::const_iterator it =
                    ctx -> events -> cbegin();
                it != ctx -> events -> cend();
                ++it
            ) {

                in_packet_r_ctx_event* event = *it;

                char* r_id = (char*)malloc( 21 );
                sprintf( r_id, "%llu", max_id );

                size_t r_id_len = strlen( r_id );
                uint32_t key_len =
                    4 // rin: | rts: | rid: | rpr:
                    + ctx -> event_name_len
                    + 1 // :
                    + _peer_id_len
                    + 1 // :
                    + r_id_len
                ;
                char* key = (char*)malloc( key_len );

                sprintf( key, "rin:" );
                memcpy( key + 4, ctx -> event_name, ctx -> event_name_len );
                key[ 4 + ctx -> event_name_len ] = ':';

                memcpy( key + 4 + ctx -> event_name_len + 1, _peer_id,
                    _peer_id_len );
                key[ 4 + ctx -> event_name_len + 1 + _peer_id_len ] = ':';

                memcpy( key + 4 + ctx -> event_name_len + 1 + _peer_id_len + 1,
                    r_id, r_id_len );

                if( db.add( key, key_len, event -> data, event -> len ) ) {

                    key[ 1 ] = 't';
                    key[ 2 ] = 's';

                    if( db.add( key, key_len, now, now_len ) ) {

                        key[ 1 ] = 'i';
                        key[ 2 ] = 'd';

                        // printf("about to save rid: %llu\n",ntohll(event -> id_net));

                        if( db.add( key, key_len, (char*)&(event -> id_net),
                            sizeof( event -> id_net ) ) ) {

                            bool rpr_ok = false;

                            if( peers_cnt > 0 ) {

                                key[ 1 ] = 'p';
                                key[ 2 ] = 'r';

                                rpr_ok = db.add(
                                    key, key_len,
                                    serialized_peers,
                                    serialized_peers_len
                                );

                                if( ! rpr_ok )
                                    fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );

                            } else {

                                rpr_ok = true;
                            }

                            if( rpr_ok ) {

                                key[ 1 ] = 'r';
                                key[ 2 ] = 'e';

                                size_t event_id_len = strlen( event -> id );
                                key_len -= r_id_len;
                                key = (char*)realloc( key, key_len + event_id_len );

                                memcpy( key + key_len, event -> id, event_id_len );
                                key_len += event_id_len;

                                auto _max_id = htonll( max_id );
// printf("repl_save: before db.add(%s)\n", key);
                                if( db.add( key, key_len, (char*)&_max_id, sizeof( _max_id ) ) ) {

                                    free( key );
                                    free( r_id );
                                    ++num_inserted;
                                    --max_id;

                                } else {

                                    fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );
                                    free( key );
                                    free( r_id );
                                    break;
                                }

                            } else {

                                free( key );
                                free( r_id );
                                break;
                            }

                        } else {

                            fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );
                            free( key );
                            free( r_id );
                            break;
                        }

                    } else {

                        fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );
                        free( key );
                        free( r_id );
                        break;
                    }

                } else {

                    fprintf( stderr, "db.add(%s) failed: %s\n", key, db.error().name() );
                    free( key );
                    free( r_id );
                    break;
                }
            }

            if( num_inserted == ctx -> events -> size() ) {

                if( db.end_transaction( true ) ) {

                    pthread_mutex_lock( &stat_mutex );
                    stat_num_replications += num_inserted;
                    pthread_mutex_unlock( &stat_mutex );

                    result = REPL_SAVE_RESULT_K;

                } else {

                    fprintf( stderr, "Failed to commit transaction: %s\n", db.error().name() );
                    exit( 1 );
                }

            } else {

                if( ! db.end_transaction( false ) ) {

                    fprintf( stderr, "Failed to abort transaction: %s\n", db.error().name() );
                    exit( 1 );
                }
            }

            free( now );
        }

        free( increment_key );
    }

    free( serialized_peers );

    while( ! ctx -> peers -> empty() ) {

        packet_r_ctx_peer* peer = ctx -> peers -> back();
        ctx -> peers -> pop_back();

        free( peer -> hostname );
        free( peer );
    }

    free( ctx -> peers );

    while( ! ctx -> events -> empty() ) {

        in_packet_r_ctx_event* event = ctx -> events -> back();
        ctx -> events -> pop_back();

        free( event -> data );
        free( event );
    }

    free( ctx -> events );
    free( ctx -> hostname );
    free( ctx -> event_name );

    return result;
}

static inline void continue_replication_exec( out_packet_i_ctx*& ctx ) {

    if( *(ctx -> pending) == 0 ) {

        pthread_mutex_lock( &replication_exec_queue_mutex );

        replication_exec_queue.push( ctx );

        pthread_mutex_unlock( &replication_exec_queue_mutex );
    }
}

static inline void begin_replication( out_packet_r_ctx*& r_ctx ) {

    Client* peer = NULL;

    while( ( peer == NULL ) && ( r_ctx -> candidate_peer_ids -> size() > 0 ) ) {

        char* peer_id = r_ctx -> candidate_peer_ids -> back();
        r_ctx -> candidate_peer_ids -> pop_back();

        pthread_mutex_lock( &known_peers_mutex );

        known_peers_t::const_iterator it = known_peers.find( peer_id );

        if( it != known_peers.cend() )
            peer = it -> second;

        pthread_mutex_unlock( &known_peers_mutex );
    }

    bool done = false;

    if( peer == NULL ) {

        if( r_ctx -> sync ) {

            uint32_t accepted_peers_count = r_ctx -> accepted_peers -> size();

            if( accepted_peers_count >= r_ctx -> replication_factor ) {

                if( r_ctx -> client != NULL ) {

                    char* r_ans = (char*)malloc( 1 );
                    r_ans[ 0 ] = 'k';
                    r_ctx -> client -> push_write_queue( 1, r_ans, NULL );
                }

                r_ctx -> sync = false;

            } else if( r_ctx -> pending == 0 ) {

                if( r_ctx -> client != NULL ) {

                    char* r_ans = (char*)malloc( 1 );
                    r_ans[ 0 ] = 'a';
                    r_ctx -> client -> push_write_queue( 1, r_ans, NULL );
                }

                r_ctx -> sync = false;
            }
        }

        if( r_ctx -> pending == 0 )
            done = true;

    } else {

        uint32_t accepted_peers_count = r_ctx -> accepted_peers -> size();

        if(
            r_ctx -> sync
            && ( accepted_peers_count >= r_ctx -> replication_factor )
        ) {

            if( r_ctx -> client != NULL ) {

                char* r_ans = (char*)malloc( 1 );
                r_ans[ 0 ] = 'k';
                r_ctx -> client -> push_write_queue( 1, r_ans, NULL );
            }

            r_ctx -> sync = false;
        }

        if( accepted_peers_count >= max_replication_factor ) {

            done = true;

        } else {

            size_t r_len = 0;
            char* r_req = (char*)malloc( r_ctx -> r_len + sizeof( accepted_peers_count ) );

            memcpy( r_req + r_len, r_ctx -> r_req, r_ctx -> r_len );
            r_len += r_ctx -> r_len;

            uint32_t _accepted_peers_count = htonl( accepted_peers_count );
            memcpy( r_req + r_len, &_accepted_peers_count, sizeof( _accepted_peers_count ) );
            r_len += sizeof( _accepted_peers_count );

            for(
                std::list<packet_r_ctx_peer*>::const_iterator it =
                    r_ctx -> accepted_peers -> cbegin();
                it != r_ctx -> accepted_peers -> cend();
                ++it
            ) {

                packet_r_ctx_peer* peer = *it;

                r_req = (char*)realloc( r_req, r_len
                    + sizeof( peer -> hostname_len )
                    + peer -> hostname_len
                    + sizeof( peer -> port )
                );

                uint32_t _len = htonl( peer -> hostname_len );
                memcpy( r_req + r_len, &_len, sizeof( _len ) );
                r_len += sizeof( _len );

                memcpy( r_req + r_len, peer -> hostname, peer -> hostname_len );
                r_len += peer -> hostname_len;

                memcpy( r_req + r_len, &(peer -> port),
                    sizeof( peer -> port ) );
                r_len += sizeof( peer -> port );
            }

            ++(r_ctx -> pending);

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = 1;
            item -> cb = &Client::replication_cb;
            item -> ctx = (void*)r_ctx;
            item -> err = &Client::replication_skip_peer;
            item -> opcode = true;

            peer -> push_write_queue( r_len, r_req, item );
        }
    }

    if( done ) {

        while( ! r_ctx -> accepted_peers -> empty() ) {

            packet_r_ctx_peer* peer =
                r_ctx -> accepted_peers -> back();

            r_ctx -> accepted_peers -> pop_back();

            free( peer );
        }

        free( r_ctx -> accepted_peers );
        free( r_ctx -> candidate_peer_ids );
        free( r_ctx -> r_req );
        free( r_ctx );
    }
}

static void client_cb( struct ev_loop* loop, ev_io* _watcher, int events ) {

    struct client_bound_ev_io* watcher = (struct client_bound_ev_io*)_watcher;
    Client* client = watcher -> client;

    if( events & EV_ERROR ) {

        printf( "EV_ERROR!\n" );

        delete client;

        return;
    }

    if( events & EV_READ ) {

        char* buf = (char*)malloc( read_size );

        int read = recv( _watcher -> fd, buf, read_size, 0 );

        if( read > 0 ) {

            // for(int i = 0; i < read; ++i)
            //     printf("read from %s: 0x%.2X\n", client -> get_peer_id(),buf[i]);

            client -> push_read_queue( read, buf );
            free( buf );

        } else if( read < 0 ) {

            if( ( errno != EAGAIN ) && ( errno != EINTR ) ) {

                perror( "recv" );
                free( buf );
                delete client;

                return;
            }

        } else {

            free( buf );
            delete client;

            return;
        }
    }

    if( events & EV_WRITE ) {

        WriteQueueItem* item = client -> get_pending_write();

        if( item != NULL ) {

            int written = write(
                _watcher -> fd,
                ( item -> data + item -> pos ),
                ( item -> len - item -> pos )
            );

            if( written < 0 ) {

                if( ( errno != EAGAIN ) && ( errno != EINTR ) ) {

                    perror( "write" );
                    delete client;

                    return;
                }

            } else {

                // for(int i = 0; i < written; ++i)
                //     printf("written to %s: 0x%.2X\n", client -> get_peer_id(),((char*)(item->data + item -> pos))[i]);

                item -> pos += written;

                if(
                    (item -> pos >= item -> len)
                    && (item -> cb != NULL)
                ) {

                    client -> push_pending_reads_queue( item -> cb );
                    item -> cb = NULL;
                }
            }
        }
    }

    return;
}

static void* client_thread( void* args ) {

    struct ev_loop* loop = ev_loop_new( 0 );

    while( true ) {

        ev_run( loop, EVRUN_NOWAIT );

        if( ! new_clients.empty() ) {

            pthread_mutex_lock( &new_clients_mutex );

            if( ! new_clients.empty() ) {

                new_client_t* new_client = new_clients.front();
                new_clients.pop();

                pthread_mutex_unlock( &new_clients_mutex );

                Client* client = new Client(
                    new_client -> fh,
                    loop,
                    new_client -> s_in,
                    new_client -> s_in_len
                );

                if( new_client -> cb != NULL )
                    new_client -> cb( client );

                free( new_client );

            } else {

                pthread_mutex_unlock( &new_clients_mutex );
            }
        }
    }

    return NULL;
}

static void discovery_cb1( Client* client ) {

    char* w_req = (char*)malloc( 1 );
    w_req[ 0 ] = 'w';

    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

    item -> len = 1;
    item -> cb = &Client::discovery_cb2;
    item -> ctx = NULL;
    item -> err = NULL;
    item -> opcode = true;

    client -> push_write_queue( 1, w_req, item );
}

static inline void repl_clean(
    size_t failover_key_len,
    const char* failover_key,
    uint64_t wrinseq
) {

    size_t failover_key_slen = strlen( failover_key );
    std::vector<std::string> keys;

    std::string rre_key( "rre:", 4 );
    rre_key.append( failover_key, failover_key_slen );

    keys.push_back( rre_key );

    char* suffix = (char*)malloc( failover_key_len );
    memcpy( suffix, failover_key, failover_key_len );
    sprintf( suffix + failover_key_len - 20 - 1, "%llu", wrinseq );
    failover_key_slen = strlen( suffix );

    std::string rin_key( "rin:", 4 );
    rin_key.append( suffix, failover_key_len );

    std::string rts_key( "rts:", 4 );
    rts_key.append( suffix, failover_key_len );

    std::string rid_key( "rid:", 4 );
    rid_key.append( suffix, failover_key_len );

    std::string rpr_key( "rpr:", 4 );
    rpr_key.append( suffix, failover_key_len );

    keys.push_back( rin_key );
    keys.push_back( rts_key );
    keys.push_back( rid_key );
    keys.push_back( rpr_key );

    if( db.remove_bulk( keys ) == -1 )
        fprintf( stderr, "db.remove_bulk failed: %s\n", db.error().name() );
}

static void* replication_exec_thread( void* args ) {

    while( true ) {

        if( replication_exec_queue.size() == 0 ) continue;

        pthread_mutex_lock( &replication_exec_queue_mutex );

        // TODO: persistent queue
        out_packet_i_ctx* ctx = replication_exec_queue.front();
        replication_exec_queue.pop();

        pthread_mutex_unlock( &replication_exec_queue_mutex );

        printf( "Replication exec thread for task %llu\n", ctx -> rid );

        if( ctx -> acceptances == ctx -> count_replicas ) {

            {
                failover_t::const_iterator it = failover.find( ctx -> failover_key );

                if( it == failover.cend() ) {

                    // TODO: cleanup
                    continue;
                }
            }

            {
                no_failover_t::const_iterator it = no_failover.find( ctx -> failover_key );

                if( it != no_failover.cend() ) {

                    if( ( it -> second + no_failover_time ) > std::time( nullptr ) ) {

                        // TODO: cleanup
                        continue;

                    } else {

                        no_failover.erase( it );
                    }
                }
            }

            in_packet_e_ctx_event* event = (in_packet_e_ctx_event*)malloc(
                sizeof( *event ) );

            event -> len = ctx -> data -> len;
            event -> data = ctx -> data -> data;

            in_packet_e_ctx* e_ctx = (in_packet_e_ctx*)malloc( sizeof( *e_ctx ) );

            e_ctx -> cnt = 1;
            e_ctx -> event_name_len = ctx -> event -> id_len;
            e_ctx -> event_name = ctx -> event -> id;
            e_ctx -> events = new std::list<in_packet_e_ctx_event*>();
            e_ctx -> events -> push_back( event );

            std::vector<uint64_t> task_ids;
            save_event( e_ctx, 0, NULL, &task_ids );

            Client::free_in_packet_e_ctx( (void*)e_ctx );

            failover[ ctx -> failover_key ] = task_ids.front();

            repl_clean(
                ctx -> failover_key_len,
                ctx -> failover_key,
                ctx -> wrinseq
            );

            if( ctx -> rpr != NULL ) {

                size_t x_req_len = 1;
                char* x_req = (char*)malloc(
                    x_req_len
                    + sizeof( ctx -> peer_id -> len )
                    + ctx -> peer_id -> len
                    + ctx -> event -> id_len_size
                    + ctx -> event -> id_len
                    + sizeof( ctx -> rid )
                );

                x_req[ 0 ] = 'x';

                uint32_t peer_id_len_net = htonl( ctx -> peer_id -> len );
                memcpy( x_req + x_req_len, &peer_id_len_net, sizeof( peer_id_len_net ) );
                x_req_len += sizeof( peer_id_len_net );

                memcpy( x_req + x_req_len, ctx -> peer_id -> data, ctx -> peer_id -> len );
                x_req_len += ctx -> peer_id -> len;

                memcpy( x_req + x_req_len, &(ctx -> event -> id_len_net),
                    ctx -> event -> id_len_size );
                x_req_len += ctx -> event -> id_len_size;

                memcpy( x_req + x_req_len, ctx -> event -> id,
                    ctx -> event -> id_len );
                x_req_len += ctx -> event -> id_len;

                uint64_t rid_net = htonll( ctx -> rid );
                memcpy( x_req + x_req_len, &rid_net, sizeof( rid_net ) );
                x_req_len += sizeof( rid_net );

                size_t offset = 0;

                while( ctx -> peers_cnt > 0 ) {

                    size_t peer_id_len = strlen( ctx -> rpr + offset );
                    char* peer_id = (char*)malloc( peer_id_len + 1 );
                    memcpy( peer_id, ctx -> rpr + offset, peer_id_len );
                    peer_id[ peer_id_len ] = '\0';
                    offset += peer_id_len + 1;

                    known_peers_t::const_iterator it = known_peers.find( peer_id );

                    if( it != known_peers.cend() ) {

                        it -> second -> push_write_queue( x_req_len, x_req, NULL );
                    }

                    --(ctx -> peers_cnt);
                }
            }

        } else {

            // TODO: perl
            // unless( $clean -> () ) {
            //
            //     warn( 'remove_bulk(): ' . $db -> error() );
            // }
            //
            // $unfailover -> ();

            // TODO: think about repl_clean()

            free( ctx -> data -> data );
            free( ctx -> data );
        }

        pthread_mutex_destroy( ctx -> mutex );

        free( ctx -> mutex );
        free( ctx -> acceptances );
        free( ctx -> pending );
        free( ctx -> count_replicas );
        free( ctx );
    }

    return NULL;
}

static void* replication_thread( void* args ) {

    while( true ) {

        std::vector<muh_str_t*> peer_ids;

        pthread_mutex_lock( &peers_to_discover_mutex );

        for(
            peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
            it != peers_to_discover.cend();
            ++it
        ) {

            muh_str_t* item = (muh_str_t*)malloc( sizeof( *item ) );

            item -> len = strlen( it -> first );
            item -> data = it -> first;

            peer_ids.push_back( item );
        }

        pthread_mutex_unlock( &peers_to_discover_mutex );

        std::random_shuffle( peer_ids.begin(), peer_ids.end() );

        known_peers_t::const_iterator _peer;
        Client* peer;
        get_keys_result_t* dbdata;
        std::vector<std::string> keys;
        uint64_t now = std::time( nullptr );

        for(
            known_events_t::const_iterator _event = known_events.cbegin();
            _event != known_events.cend();
            ++_event
        ) {

            known_event_t* event = _event -> second;
            // printf("repl thread: %s\n", event -> id);

            for(
                std::vector<muh_str_t*>::const_iterator _peer_id = peer_ids.cbegin();
                _peer_id != peer_ids.cend();
                ++_peer_id
            ) {

                size_t suffix_len =
                    event -> id_len
                    + 1 // :
                    + (*_peer_id) -> len
                ;

                char* suffix = (char*)malloc(
                    suffix_len
                    + 1 // :
                    + 20 // wrinseq
                    + 1 // \0
                );
                sprintf( suffix, "%s:%s", event -> id, (*_peer_id) -> data );
                // printf("repl thread: %s\n", suffix);

                std::string wrinseq_key( "wrinseq:", 8 );
                wrinseq_key.append( suffix, suffix_len );

                std::string rinseq_key( "rinseq:", 7 );
                rinseq_key.append( suffix, suffix_len );

                keys.push_back( wrinseq_key );
                keys.push_back( rinseq_key );
// printf("replication_thread: before first db_get_keys\n");
                dbdata = db_get_keys( keys );
                // printf("replication_thread: after first db_get_keys\n");
                keys.clear();

                if( dbdata == NULL ) {

                    fprintf( stderr, "db.accept_bulk failed: %s\n", db.error().name() );
                    exit( 1 );
                }

                uint64_t rinseq;
                uint64_t wrinseq;

                auto next = [ &wrinseq_key, &wrinseq ](){

                    uint64_t next_wrinseq = htonll( wrinseq + 1 );
                    uint64_t __wrinseq = htonll( wrinseq );
// printf("replication_thread: before db.cas()\n");
                    if( ! db.cas(
                        wrinseq_key.c_str(),
                        wrinseq_key.length(),
                        (char*)&__wrinseq,
                        sizeof( __wrinseq ),
                        (char*)&next_wrinseq,
                        sizeof( next_wrinseq )
                    ) ) {

                        fprintf( stderr, "db.cas(%s,%llu,%llu) failed: %s\n", wrinseq_key.c_str(), wrinseq, ntohll( next_wrinseq ), db.error().name() );
                        exit( 1 );
                    }
// printf("replication_thread: after db.cas()\n");
                };

                {
                    uint64_t* _rinseq = parse_db_value<uint64_t>( dbdata, &rinseq_key );
                    uint64_t* _wrinseq = parse_db_value<uint64_t>( dbdata, &wrinseq_key );

                    if( _rinseq == NULL ) rinseq = 0;
                    else {
                        rinseq = ntohll( *_rinseq );
                        free( _rinseq );
                    }

                    if( _wrinseq == NULL ) {

                        wrinseq = 0;
                        uint64_t __wrinseq = htonll( wrinseq );

                        if( ! db.add(
                            wrinseq_key.c_str(),
                            wrinseq_key.length(),
                            (char*)&__wrinseq,
                            sizeof( __wrinseq )
                        ) ) {
                            auto error = db.error();
                            fprintf( stderr, "db.add(%s) failed: %s\n", wrinseq_key.c_str(), error.name() );

                            if( error.code() == kyotocabinet::BasicDB::Error::Code::DUPREC ) {

                                next();
                                continue;

                            } else {

                                exit( 1 );
                            }
                        }

                    } else {

                        wrinseq = ntohll( *_wrinseq );
                        free( _wrinseq );
                    }
                }

                delete dbdata;

                if( wrinseq >= rinseq ) {

                    // printf("Skip repl: %llu >= %llu\n", wrinseq, rinseq);
                    free( suffix );
                    continue;
                }

                suffix[ suffix_len ] = ':';
                ++suffix_len;

                sprintf( suffix + suffix_len, "%llu", wrinseq );
                suffix_len += 20;
                size_t suffix_slen = strlen( suffix );

                std::string rin_key( "rin:", 4 );
                rin_key.append( suffix, suffix_slen );

                std::string rts_key( "rts:", 4 );
                rts_key.append( suffix, suffix_slen );

                std::string rid_key( "rid:", 4 );
                rid_key.append( suffix, suffix_slen );

                std::string rpr_key( "rpr:", 4 );
                rpr_key.append( suffix, suffix_slen );

                keys.push_back( rin_key );
                keys.push_back( rts_key );
                keys.push_back( rid_key );
                keys.push_back( rpr_key );

                // for(
                //     std::vector<std::string>::const_iterator it = keys.cbegin();
                //     it != keys.cend();
                //     ++it
                // ) {
                //
                //     printf( "gotta ask for (%lu bytes) %s\n", it -> size(), it -> c_str() );
                // }

                dbdata = db_get_keys( keys );
                keys.clear();

                if( dbdata == NULL ) {

                    fprintf( stderr, "db.accept_bulk failed: %s\n", db.error().name() );
                    exit( 1 );
                }

                uint32_t rin_len;
                size_t _rin_len;
                char* rin = parse_db_value<char>( dbdata, &rin_key, &_rin_len );
                rin_len = _rin_len;

                if( rin == NULL ) {

                    fprintf( stderr, "No data for replicated event: %s, rin_key: %s\n", suffix, rin_key.c_str() );
                    next();
                    continue;
                }

                uint64_t rts;
                uint64_t rid;
                uint64_t rid_net;

                {
                    uint64_t* _rts = parse_db_value<uint64_t>( dbdata, &rts_key );
                    uint64_t* _rid = parse_db_value<uint64_t>( dbdata, &rid_key );

                    if( _rts == NULL ) {

                        fprintf( stderr, "No timestamp for replicated event: %s\n", suffix );
                        next();
                        continue;

                    } else {

                        rts = ntohll( *_rts );
                        // free( _rts );
                    }

                    if( _rid == NULL ) {

                        fprintf( stderr, "No remote id for replicated event: %s\n", suffix );
                        next();
                        continue;

                    } else {

                        rid_net = *_rid;
                        // free( _rid );
                        rid = ntohll( rid_net );
                    }
                }

                size_t rpr_len;
                char* rpr = parse_db_value<char>( dbdata, &rpr_key, &rpr_len );

                delete dbdata;

                if( ( rts + event -> ttl ) > now ) {

                    // printf("skip repl: not now\n");
                    free( rin );
                    if( rpr != NULL ) free( rpr );
                    free( suffix );
                    continue;
                }

                char* failover_key = suffix;
                sprintf( failover_key + suffix_len - 20 - 1, "%llu", rid );

                {
                    failover_t::const_iterator it = failover.find( failover_key );

                    if( it != failover.cend() ) {

                        free( rin );
                        if( rpr != NULL ) free( rpr );
                        // free( suffix );
                        free( failover_key );
                        continue;
                    }
                }

                {
                    no_failover_t::const_iterator it = no_failover.find( failover_key );

                    if( it != no_failover.cend() ) {

                        if( ( it -> second + no_failover_time ) > now ) {

                            free( rin );
                            if( rpr != NULL ) free( rpr );
                            // free( suffix );
                            free( failover_key );
                            continue;

                        } else {

                            no_failover.erase( it );
                        }
                    }
                }

                failover[ failover_key ] = 0;

                pthread_mutex_lock( &known_peers_mutex );

                _peer = known_peers.find( (*_peer_id) -> data );

                if( _peer == known_peers.cend() ) peer = NULL;
                else peer = _peer -> second;

                pthread_mutex_unlock( &known_peers_mutex );

                printf( "Seems like I need to failover task %llu\n", rid );

                if( peer == NULL ) {

                    size_t offset = 0;
                    uint32_t peers_cnt = 0;
                    bool have_rpr = false;

                    uint32_t* count_replicas = (uint32_t*)malloc( sizeof(
                        *count_replicas ) );
                    uint32_t* acceptances = (uint32_t*)malloc( sizeof(
                        *acceptances ) );
                    uint32_t* pending = (uint32_t*)malloc( sizeof(
                        *pending ) );

                    *count_replicas = 0;
                    *acceptances = 0;
                    *pending = 0;

                    pthread_mutex_t* mutex = (pthread_mutex_t*)malloc( sizeof( *mutex ) );
                    pthread_mutex_init( mutex, NULL );

                    muh_str_t* data_str = (muh_str_t*)malloc( sizeof( *data_str ) );

                    data_str -> len = rin_len;
                    data_str -> data = rin;

                    if( rpr != NULL ) {

                        uint32_t _peers_cnt;
                        memcpy( &_peers_cnt, rpr + offset, sizeof( _peers_cnt ) );
                        peers_cnt = ntohl( _peers_cnt );

                        *count_replicas = peers_cnt;

                        size_t i_req_len = 1;
                        char* i_req = (char*)malloc(
                            i_req_len
                            + sizeof( (*_peer_id) -> len )
                            + (*_peer_id) -> len
                            + event -> id_len_size
                            + event -> id_len
                            + sizeof( rid )
                        );

                        i_req[ 0 ] = 'i';

                        uint32_t _peer_id_len_net = htonl( (*_peer_id) -> len );
                        memcpy( i_req + i_req_len, &_peer_id_len_net,
                            sizeof( _peer_id_len_net ) );
                        i_req_len += sizeof( _peer_id_len_net );

                        memcpy( i_req + i_req_len, (*_peer_id) -> data, (*_peer_id) -> len );
                        i_req_len += (*_peer_id) -> len;

                        memcpy( i_req + i_req_len, &(event -> id_len_net),
                            event -> id_len_size );
                        i_req_len += event -> id_len_size;

                        memcpy( i_req + i_req_len, event -> id,
                            event -> id_len );
                        i_req_len += event -> id_len;

                        memcpy( i_req + i_req_len, &rid_net, sizeof( rid_net ) );
                        i_req_len += sizeof( rid_net );

                        if( peers_cnt > 0 ) {

                            have_rpr = true;

                            while( peers_cnt > 0 ) {

                                size_t peer_id_len = strlen( rpr + offset );
                                char* peer_id = (char*)malloc( peer_id_len + 1 );
                                memcpy( peer_id, rpr + offset, peer_id_len );
                                peer_id[ peer_id_len ] = '\0';
                                offset += peer_id_len + 1;

                                known_peers_t::const_iterator it = known_peers.find( peer_id );

                                if( it == known_peers.cend() ) {

                                    ++(*acceptances);

                                } else {

                                    ++(*pending);

                                    out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(
                                        sizeof( *ctx ) );

                                    ctx -> count_replicas = count_replicas;
                                    ctx -> pending = pending;
                                    ctx -> acceptances = acceptances;
                                    ctx -> mutex = mutex;
                                    ctx -> event = event;
                                    ctx -> data = data_str;
                                    ctx -> peer_id = *_peer_id;
                                    ctx -> wrinseq = wrinseq;
                                    ctx -> failover_key = failover_key;
                                    ctx -> failover_key_len = suffix_len;
                                    ctx -> rpr = rpr;
                                    ctx -> peers_cnt = peers_cnt;
                                    ctx -> rid = rid;

                                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                                        sizeof( *item ) );

                                    item -> len = 1;
                                    item -> cb = &Client::propose_self_k_cb;
                                    item -> ctx = (void*)ctx;
                                    item -> err = &Client::propose_self_f_cb;
                                    item -> opcode = true;

                                    it -> second -> push_write_queue( i_req_len, i_req, item );
                                }

                                --peers_cnt;
                            }
                        }
                    }

                    if( ! have_rpr ) {

                        out_packet_i_ctx* ctx = (out_packet_i_ctx*)malloc(
                            sizeof( *ctx ) );

                        ctx -> count_replicas = count_replicas;
                        ctx -> pending = pending;
                        ctx -> acceptances = acceptances;
                        ctx -> mutex = mutex;
                        ctx -> event = event;
                        ctx -> data = data_str;
                        ctx -> peer_id = *_peer_id;
                        ctx -> wrinseq = wrinseq;
                        ctx -> failover_key = failover_key;
                        ctx -> failover_key_len = suffix_len;
                        ctx -> rpr = rpr;
                        ctx -> peers_cnt = 0;
                        ctx -> rid = rid;

                        replication_exec_queue.push( ctx );
                    }

                } else {

                    size_t c_req_len = 1;
                    char* c_req = (char*)malloc( c_req_len
                        + event -> id_len_size
                        + event -> id_len
                        + sizeof( rid_net )
                        + sizeof( rin_len )
                        + rin_len
                    );

                    c_req[ 0 ] = 'c';

                    memcpy( c_req + c_req_len, (char*)&(event -> id_len_net),
                        event -> id_len_size );
                    c_req_len += event -> id_len_size;

                    memcpy( c_req + c_req_len, event -> id, event -> id_len );
                    c_req_len += event -> id_len;

                    memcpy( c_req + c_req_len, (char*)&rid_net, sizeof( rid_net ) );
                    c_req_len += sizeof( rid_net );

                    uint32_t rin_len_net = htonl( rin_len );
                    memcpy( c_req + c_req_len, (char*)&rin_len_net, sizeof( rin_len_net ) );
                    c_req_len += sizeof( rin_len_net );

                    memcpy( c_req + c_req_len, rin, rin_len );
                    c_req_len += rin_len;

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                        sizeof( *item ) );

                    muh_str_t* rin_str = (muh_str_t*)malloc( sizeof( *rin_str ) );

                    rin_str -> len = rin_len;
                    rin_str -> data = rin;

                    muh_str_t* rpr_str = NULL;

                    if( rpr != NULL ) {

                        rpr_str = (muh_str_t*)malloc( sizeof( *rpr_str ) );
                        rpr_str -> len = strlen( rpr );
                        rpr_str -> data = rpr;
                    }

                    out_packet_c_ctx* ctx = (out_packet_c_ctx*)malloc( sizeof( *ctx ) );

                    ctx -> client = peer;
                    ctx -> event = event;
                    ctx -> rin = rin_str;
                    ctx -> rpr = rpr_str;
                    ctx -> rid = rid;
                    ctx -> wrinseq = wrinseq;
                    ctx -> failover_key = failover_key;
                    ctx -> failover_key_len = suffix_len;

                    item -> len = 1;
                    item -> cb = &Client::ping_task_k_cb;
                    item -> ctx = (void*)ctx;
                    item -> err = &Client::ping_task_f_cb;
                    item -> opcode = true;

                    peer -> push_write_queue( c_req_len, c_req, item );
                }

                next();
            }
        }
    }

    return NULL;
}

static void* discovery_thread( void* args ) {

    while( true ) {

        for(
            peers_to_discover_t::const_iterator it = peers_to_discover.cbegin();
            it != peers_to_discover.cend();
            ++it
        ) {

            peer_to_discover_t* peer_to_discover = it -> second;

            addrinfo hints;
            addrinfo* service_info;

            memset( &hints, 0, sizeof( hints ) );
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_NUMERICSERV;

            char port[ 6 ];
            sprintf( port, "%d", peer_to_discover -> port );

            int rv;

            if( ( rv = getaddrinfo( peer_to_discover -> host, port, &hints, &service_info ) ) != 0 ) {

                fprintf( stderr, "getaddrinfo(%s, %u): %s\n",
                    peer_to_discover -> host, peer_to_discover -> port, gai_strerror( rv ) );
                continue;
            }

            int fh;
            int yes = 1;
            sockaddr_in* addr = (sockaddr_in*)malloc( sizeof( *addr ) );
            socklen_t addr_len;
            bool connected = false;

            for( addrinfo* ai_it = service_info; ai_it != NULL; ai_it = ai_it -> ai_next ) {

                if( ( fh = socket( ai_it -> ai_family, ai_it -> ai_socktype, ai_it -> ai_protocol ) ) == -1 ) {

                    perror( "socket" );
                    continue;
                }

                if( setsockopt( fh, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof( yes ) ) == -1 ) {

                    perror( "setsockopt" );
                    close( fh );
                    continue;
                }

                if( setsockopt( fh, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof( yes ) ) == -1 ) {

                    perror( "setsockopt" );
                    close( fh );
                    continue;
                }

                timeval tv;
                tv.tv_sec = ( discovery_timeout_milliseconds / 1000 );
                tv.tv_usec = ( ( discovery_timeout_milliseconds % 1000 ) * 1000 );

                if( setsockopt( fh, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof( tv ) ) == -1 ) {

                    perror( "setsockopt" );
                    close( fh );
                    continue;
                }

                if( setsockopt( fh, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof( tv ) ) == -1 ) {

                    perror( "setsockopt" );
                    close( fh );
                    continue;
                }

                if( connect( fh, ai_it -> ai_addr, ai_it -> ai_addrlen ) == -1 ) {

                    if( errno != EINPROGRESS ) {

                        perror( "connect" );
                        close( fh );
                        continue;
                    }

                    fcntl( fh, F_SETFL, fcntl( fh, F_GETFL, 0 ) | O_NONBLOCK );
                }

                *addr = *(sockaddr_in*)(ai_it -> ai_addr);
                addr_len = ai_it -> ai_addrlen;
                connected = true;

                break;
            }

            freeaddrinfo( service_info );

            if( connected ) {

                if( getpeername( fh, (sockaddr*)addr, &addr_len ) == -1 ) {

                    perror( "getpeername" );
                    close( fh );
                    free( addr );

                } else {

                    char* conn_name = get_host_from_sockaddr_in( addr );
                    uint32_t conn_port = get_port_from_sockaddr_in( addr );
                    char* conn_id = make_peer_id( strlen( conn_name ), conn_name, conn_port );

                    free( conn_name );
                    bool found = false;

                    {
                        pthread_mutex_lock( &known_peers_mutex );

                        known_peers_t::const_iterator it = known_peers_by_conn_id.find( conn_id );

                        if( it != known_peers_by_conn_id.cend() )
                            found = true;

                        pthread_mutex_unlock( &known_peers_mutex );
                    }

                    if( ! found ) {

                        pthread_mutex_lock( &known_peers_mutex );

                        known_peers_t::const_iterator it = known_peers.find( conn_id );

                        if( it != known_peers.cend() )
                            found = true;

                        pthread_mutex_unlock( &known_peers_mutex );
                    }

                    if( ! found ) {

                        pthread_mutex_lock( &me_mutex );

                        me_t::const_iterator it = me.find( conn_id );

                        if( it != me.cend() )
                            found = true;

                        pthread_mutex_unlock( &me_mutex );
                    }

                    free( conn_id );

                    if( found ) {

                        shutdown( fh, SHUT_RDWR );
                        close( fh );
                        free( addr );

                    } else {

                        new_client_t* new_client = (new_client_t*)malloc(
                            sizeof( *new_client ) );

                        new_client -> fh = fh;
                        new_client -> cb = discovery_cb1;
                        new_client -> s_in = addr;
                        new_client -> s_in_len = addr_len;

                        pthread_mutex_lock( &new_clients_mutex );
                        new_clients.push( new_client );
                        pthread_mutex_unlock( &new_clients_mutex );
                    }
                }

            } else {

                fprintf( stderr, "Peer %s is unreachable\n", it -> first );
                free( addr );
            }
        }

        sleep( 5 );
    }

    return NULL;
}

static void socket_cb( struct ev_loop* loop, ev_io* watcher, int events ) {

    sockaddr_in* addr = (sockaddr_in*)malloc( sizeof( *addr ) );
    socklen_t len = sizeof( *addr );

    int fh = accept( watcher -> fd, (sockaddr*)addr, &len );

    if( fh < 0 ) {

        perror( "accept" );
        free( addr );
        return;
    }

    new_client_t* new_client = (new_client_t*)malloc( sizeof( *new_client ) );

    new_client -> fh = fh;
    new_client -> cb = NULL;
    new_client -> s_in = addr;
    new_client -> s_in_len = len;

    pthread_mutex_lock( &new_clients_mutex );
    new_clients.push( new_client );
    pthread_mutex_unlock( &new_clients_mutex );

    return;
}

static void* synchronization_thread( void* args ) {

    while( true ) {

        sleep( 1 );
        db.synchronize();

        pthread_mutex_lock( &stat_mutex );

        if( stat_num_inserts > 0 )
            printf( "number of inserts for last second: %llu\n", stat_num_inserts );

        if( stat_num_replications > 0 )
            printf( "number of replication inserts for last second: %llu\n",
                stat_num_replications );

        stat_num_inserts = 0;
        stat_num_replications = 0;

        pthread_mutex_unlock( &stat_mutex );
    }

    return NULL;
}

int main( int argc, char** argv ) {

    std::string db_file_name;
    std::string known_events_file_name;

    try {

        TCLAP::CmdLine cmd( "skree", '=', "0.01" );

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

        cmd.add( _port );
        cmd.add( _max_client_threads );
        cmd.add( _db_file_name );
        cmd.add( _known_events_file_name );

        cmd.parse( argc, argv );

        my_port = _port.getValue();
        max_client_threads = _max_client_threads.getValue();
        db_file_name = _db_file_name.getValue();
        known_events_file_name = _known_events_file_name.getValue();

    } catch( TCLAP::ArgException& e ) {

        printf( "%s %s\n", e.error().c_str(), e.argId().c_str() );
    }

    YAML::Node config = YAML::LoadFile( known_events_file_name );

    {
        if( config.Type() != YAML::NodeType::Sequence ) {

            fprintf( stderr, "Known events file should contain a sequence of event groups\n" );
        }

        for( YAML::const_iterator group = config.begin(); group != config.end(); ++group ) {

            if( group -> Type() != YAML::NodeType::Map ) {

                fprintf( stderr, "Each event group should be a map\n" );
                exit( 1 );
            }

            const YAML::Node _name = (*group)[ "name" ];
            std::string group_name;

            if( _name && ( _name.Type() == YAML::NodeType::Scalar ) ) {

                group_name = _name.as<std::string>();

            } else {

                fprintf( stderr, "Every event group should have a name\n" );
                exit( 1 );
            }

            const YAML::Node _events = (*group)[ "events" ];

            if( ! _events || ( _events.Type() != YAML::NodeType::Sequence ) ) {

                fprintf( stderr, "Every event group should have an event list\n" );
                exit( 1 );
            }

            event_group_t* event_group = (event_group_t*)malloc( sizeof( *event_group ) );

            event_group -> name_len = group_name.length();

            char* group_name_ = (char*)malloc( event_group -> name_len + 1 );
            memcpy( group_name_, group_name.c_str(), event_group -> name_len );
            group_name_[ event_group -> name_len ] = '\0';

            event_group -> name = group_name_;
            // event_group -> module = skree_module; // TODO

            event_groups_t::const_iterator it = event_groups.find( group_name_ );

            if( it == event_groups.cend() ) {

                event_groups[ group_name_ ] = event_group;

            } else {

                fprintf( stderr, "Duplicate group name: %s\n", group_name_ );
                exit( 1 );
            }

            for(
                YAML::const_iterator event = _events.begin();
                event != _events.end();
                ++event
            ) {

                if( event -> Type() != YAML::NodeType::Map ) {

                    fprintf( stderr, "Every event should be a map\n" );
                    exit( 1 );
                }

                const YAML::Node _id = (*event)[ "id" ];

                if( _id && ( _id.Type() == YAML::NodeType::Scalar ) ) {

                    const YAML::Node _ttl = (*event)[ "ttl" ];
                    uint32_t ttl;

                    if( _ttl && ( _ttl.Type() == YAML::NodeType::Scalar ) ) {

                        ttl = _ttl.as<uint32_t>();

                    } else {

                        fprintf( stderr, "Every event should have a ttl\n" );
                        exit( 1 );
                    }

                    std::string id = _id.as<std::string>();

                    printf( "id: %s, group: %s, ttl: %d\n", id.c_str(), group_name.c_str(), ttl );

                    known_event_t* known_event = (known_event_t*)malloc(
                        sizeof( *known_event ) );

                    known_event -> id_len = id.length();
                    known_event -> id_len_net = htonl( known_event -> id_len );
                    known_event -> id_len_size = sizeof( known_event -> id_len );

                    char* id_ = (char*)malloc( known_event -> id_len + 1 );
                    memcpy( id_, id.c_str(), known_event -> id_len );
                    id_[ known_event -> id_len ] = '\0';

                    known_event -> id = id_;
                    known_event -> group = event_group;
                    known_event -> ttl = ttl;

                    known_events_t::const_iterator it = known_events.find( id_ );

                    if( it == known_events.cend() ) {

                        known_events[ id_ ] = known_event;

                    } else {

                        fprintf( stderr, "Duplicate event id: %s\n", id_ );
                        exit( 1 );
                    }

                } else {

                    fprintf( stderr, "Every event should have an id\n" );
                    exit( 1 );
                }
            }
        }
    }

    printf( "Running on port: %u\n", my_port );
    signal( SIGPIPE, SIG_IGN );

    if( ! db.open(
        db_file_name,
        kyotocabinet::HashDB::OWRITER
        | kyotocabinet::HashDB::OCREATE
        | kyotocabinet::HashDB::ONOLOCK
        | kyotocabinet::HashDB::OAUTOTRAN
    ) ) {

        printf( "Failed to open database: %s\n", db.error().name() );
        return 1;
    }

    load_peers_to_discover();

    my_hostname = (char*)"127.0.0.1";
    my_hostname_len = strlen( my_hostname );
    my_peer_id = make_peer_id( my_hostname_len, my_hostname, my_port );
    my_peer_id_len = strlen( my_peer_id );
    my_peer_id_len_net = htonl( my_peer_id_len );
    my_peer_id_len_size = sizeof( my_peer_id_len_net );

    sockaddr_in addr;

    int fh = socket( PF_INET, SOCK_STREAM, 0 );

    addr.sin_family = AF_UNSPEC;
    addr.sin_port = htons( my_port );
    addr.sin_addr.s_addr = INADDR_ANY;

    int yes = 1;

    if( setsockopt( fh, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof( yes ) ) == -1 ) {

        perror( "setsockopt" );
        return 1;
    }

    if( bind( fh, (sockaddr*)&addr, sizeof( addr ) ) != 0 ) {

        perror( "bind" );
        return 1;
    }

    fcntl( fh, F_SETFL, fcntl( fh, F_GETFL, 0 ) | O_NONBLOCK );
    listen( fh, 100000 );

    ev_io socket_watcher;
    struct ev_loop* loop = EV_DEFAULT;

    ev_io_init( &socket_watcher, socket_cb, fh, EV_READ );
    ev_io_start( loop, &socket_watcher );

    pthread_mutex_init( &new_clients_mutex, NULL );
    pthread_mutex_init( &known_peers_mutex, NULL );
    pthread_mutex_init( &me_mutex, NULL );
    pthread_mutex_init( &peers_to_discover_mutex, NULL );
    pthread_mutex_init( &stat_mutex, NULL );
    pthread_mutex_init( &replication_exec_queue_mutex, NULL );

    pthread_t synchronization;
    pthread_create( &synchronization, NULL, synchronization_thread, NULL );

    std::queue<pthread_t*> threads;

    for( int i = 0; i < max_client_threads; ++i ) {

        pthread_t* thread = (pthread_t*)malloc( sizeof( *thread ) );

        pthread_create( thread, NULL, client_thread, NULL );

        threads.push( thread );
    }

    {
        peer_to_discover_t* localhost7654 = (peer_to_discover_t*)malloc(
            sizeof( *localhost7654 ) );

        localhost7654 -> host = "127.0.0.1";
        localhost7654 -> port = 7654;

        peer_to_discover_t* localhost8765 = (peer_to_discover_t*)malloc(
            sizeof( *localhost8765 ) );

        localhost8765 -> host = "127.0.0.1";
        localhost8765 -> port = 8765;

        peers_to_discover[ make_peer_id(
            strlen( localhost7654 -> host ),
            (char*)localhost7654 -> host,
            localhost7654 -> port

        ) ] = localhost7654;

        peers_to_discover[ make_peer_id(
            strlen( localhost8765 -> host ),
            (char*)localhost8765 -> host,
            localhost8765 -> port

        ) ] = localhost8765;
    }

    pthread_t discovery;
    pthread_create( &discovery, NULL, discovery_thread, NULL );

    pthread_t replication;
    pthread_create( &replication, NULL, replication_thread, NULL );

    pthread_t replication_exec;
    pthread_create( &replication_exec, NULL, replication_exec_thread, NULL );

    ev_run( loop, 0 );

    pthread_join( discovery, NULL );

    while( ! threads.empty() ) {

        pthread_t* thread = threads.front();
        threads.pop();

        pthread_join( *thread, NULL );
        free( thread );
    }

    pthread_join( replication, NULL );
    pthread_join( replication_exec, NULL );
    pthread_join( synchronization, NULL );

    pthread_mutex_destroy( &known_peers_mutex );
    pthread_mutex_destroy( &new_clients_mutex );
    pthread_mutex_destroy( &me_mutex );
    pthread_mutex_destroy( &peers_to_discover_mutex );
    pthread_mutex_destroy( &stat_mutex );
    pthread_mutex_destroy( &replication_exec_queue_mutex );

    return 0;
}

#pragma clang diagnostic pop
