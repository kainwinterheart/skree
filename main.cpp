#include <map>
#include <ev.h>
#include <list>
#include <queue>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <kchashdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unordered_map>

// thank you, stackoverflow!
#ifndef htonll
#define htonll( x ) ( ( 1 == htonl( 1 ) ) ? ( x ) : ( (uint64_t)htonl( \
    ( x ) & 0xFFFFFFFF ) << 32 ) | htonl( ( x ) >> 32 ) )
#endif

#ifndef ntohll
#define ntohll( x ) ( ( 1 == ntohl( 1 ) ) ? ( x ) : ( (uint64_t)ntohl( \
    ( x ) & 0xFFFFFFFF ) << 32 ) | ntohl( ( x ) >> 32 ) )
#endif

size_t read_size = 131072;
time_t discovery_timeout_milliseconds = 3000;

uint64_t stat_num_inserts;
pthread_mutex_t stat_mutex;

kyotocabinet::HashDB db;

char* my_hostname;
uint32_t my_hostname_len;
uint32_t my_port;
char* my_peer_id;

class Client;

struct new_client_t {

    int fh;
    void (*cb) (Client*);
    sockaddr_in* s_in;
    socklen_t s_in_len;
};

std::queue<new_client_t*> new_clients;
pthread_mutex_t new_clients_mutex;

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

typedef std::unordered_map<char*, Client*, char_pointer_hasher, char_pointer_comparator> known_peers_t;
known_peers_t known_peers;
pthread_mutex_t known_peers_mutex;

typedef std::unordered_map<char*, bool, char_pointer_hasher, char_pointer_comparator> me_t;
me_t me;
pthread_mutex_t me_mutex;

struct peer_to_discover_t {

    uint32_t port;
    const char* host;
};

typedef std::unordered_map<char*, peer_to_discover_t*, char_pointer_hasher, char_pointer_comparator> peers_to_discover_t;
peers_to_discover_t peers_to_discover;
pthread_mutex_t peers_to_discover_mutex;

struct client_bound_ev_io {

    ev_io watcher;
    Client* client;
};

struct PendingReadCallbackArgs {

    size_t len;
    char* data;
    size_t* out_packet_len;
    char** out_packet;
    bool* stop;
    void* ctx;
};

struct PendingReadsQueueItem {

    size_t len;
    PendingReadsQueueItem* (Client::*cb) (PendingReadCallbackArgs*);
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

    uint32_t event_name_len;
    char* event_name;
    std::list<in_packet_e_ctx_event*>* events;
    uint32_t cnt;
};

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
        std::queue<PendingReadsQueueItem**> pending_reads;
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

                for( known_peers_t::iterator it = known_peers.begin(); it != known_peers.end(); ++it ) {

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
                item -> opcode = false;

                push_pending_reads_queue( item );

            } else if( opcode == 'r' ) {



            } else if( opcode == 'c' ) {



            } else if( opcode == 'i' ) {



            } else if( opcode == 'x' ) {



            } else if( opcode == 'h' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::packet_h_cb1;
                item -> ctx = NULL;
                item -> opcode = false;

                push_pending_reads_queue( item );

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
                            && ( ( opcode == 'k' ) || ( opcode == 'f' ) )
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

                                pending_reads.pop();
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

                        ordinary_packet_cb( opcode, &out_packet, &out_packet_len,
                            &in_packet_len );
                    }

                } else {

                    // There is no pending reads, so data should be processed
                    // as ordinary inbound packet

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

        PendingReadsQueueItem* packet_e_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_e_cb2;
            item -> ctx = NULL;
            item -> opcode = false;

            return item;
        }

        PendingReadsQueueItem* packet_e_cb2( PendingReadCallbackArgs* args ) {

            uint32_t event_name_len = args -> len - 4;

            char* event_name = (char*)malloc( event_name_len );
            memcpy( event_name, args -> data, event_name_len );

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

                return item;

            } else {

                if( args -> ctx == NULL ) {

                    return nullptr;

                } else {

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                        sizeof( *item ) );

                    item -> len = 4;
                    item -> cb = &Client::packet_e_cb6;
                    item -> ctx = args -> ctx;
                    item -> opcode = false;

                    return item;
                }
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

            uint32_t _replication_factor;
            memcpy( &_replication_factor, args -> data, args -> len );
            uint32_t replication_factor = ntohl( _replication_factor );

            in_packet_e_ctx* ctx = (in_packet_e_ctx*)(args -> ctx);

            uint64_t increment_key_len = 6 + ctx -> event_name_len;
            char* increment_key = (char*)malloc( increment_key_len );

            sprintf( increment_key, "inseq:" );
            memcpy( increment_key + 6, ctx -> event_name, ctx -> event_name_len );

            char* _out_packet = (char*)malloc( 1 );
            *(args -> out_packet_len) += 1;
            *(args -> out_packet) = _out_packet;

            _out_packet[ 0 ] = 'f';

            uint32_t _cnt = htonl( ctx -> events -> size() );
            size_t r_len = 0;
            char* r_req = (char*)malloc( 1
                + sizeof( my_hostname_len )
                + my_hostname_len
                + sizeof( my_port )
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
            memcpy( r_req + r_len, &_my_port, sizeof( _my_port ) );
            r_len += sizeof( _my_port );

            uint32_t _event_name_len = htonl( ctx -> event_name_len );
            memcpy( r_req + r_len, &_event_name_len, sizeof( _event_name_len ) );
            r_len += sizeof( _event_name_len );

            memcpy( r_req + r_len, ctx -> event_name, ctx -> event_name_len );
            r_len += ctx -> event_name_len;

            memcpy( r_req + r_len, &_cnt, sizeof( _cnt ) );
            r_len += sizeof( _cnt );

            uint32_t num_inserted = 0;

            if( db.begin_transaction() ) {

                int64_t max_id = db.increment(
                    increment_key,
                    increment_key_len,
                    ctx -> events -> size(),
                    0
                );

                if( max_id == kyotocabinet::INT64MIN ) {

                    if( ! db.end_transaction( false ) ) {

                        fprintf( stderr, "Failed to abort transaction\n" );
                        exit( 1 );
                    }

                } else {

                    for(
                        std::list<in_packet_e_ctx_event*>::iterator it =
                            ctx -> events -> begin();
                        it != ctx -> events -> end();
                        ++it
                    ) {

                        in_packet_e_ctx_event* event = *it;

                        event -> id = (char*)malloc( 21 );
                        sprintf( event -> id, "%lld", max_id );

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

                        if( db.set( key, key_len, event -> data, event -> len ) ) {

                            free( key );
                            ++num_inserted;

                        } else {

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

                        memcpy( r_req + r_len, &_max_id, sizeof( _max_id ) );
                        r_len += sizeof( _max_id );

                        memcpy( r_req + r_len, &(event -> len), sizeof( event -> len ) );
                        r_len += sizeof( event -> len );

                        memcpy( r_req + r_len, event -> data, event -> len );
                        r_len += event -> len;

                        --max_id;
                    }

                    if( num_inserted == ctx -> events -> size() ) {

                        if( db.end_transaction( true ) ) {

                            _out_packet[ 0 ] = 'k';

                            pthread_mutex_lock( &stat_mutex );
                            stat_num_inserts += num_inserted;
                            pthread_mutex_unlock( &stat_mutex );

                            // r_req
                            // r_len
                            // TODO: replication

                        } else {

                            fprintf( stderr, "Failed to commit transaction\n" );
                            exit( 1 );
                        }

                    } else {

                        if( ! db.end_transaction( false ) ) {

                            fprintf( stderr, "Failed to abort transaction\n" );
                            exit( 1 );
                        }
                    }
                }
            }

            free( increment_key );
            free( r_req );

            for(
                std::list<in_packet_e_ctx_event*>::iterator it = ctx -> events -> begin();
                it != ctx -> events -> end();
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

            known_peers_t::iterator known_peer = known_peers.find( _peer_id );

            if( known_peer == known_peers.end() ) {

                out_packet[ 0 ] = 'k';

                peer_name = (char*)malloc( host_len );
                memcpy( peer_name, host, host_len );

                peer_name_len = host_len;
                peer_port = port;
                peer_id = _peer_id;

                known_peers[ _peer_id ] = this;

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

            peers_to_discover_t::iterator prev_item = peers_to_discover.find( _peer_id );

            if( prev_item == peers_to_discover.end() ) {

                peer_to_discover_t* peer_to_discover = (peer_to_discover_t*)malloc(
                    sizeof( *peer_to_discover ) );

                peer_to_discover -> host = host;
                peer_to_discover -> port = port;

                peers_to_discover[ _peer_id ] = peer_to_discover;

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

        PendingReadsQueueItem* discovery_cb8( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::discovery_cb9;
            item -> ctx = args -> ctx;
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

                } else {

                    memcpy( args -> ctx, &_cnt, args -> len );

                    item -> ctx = args -> ctx;
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
                known_peers_t::iterator known_peer = known_peers.find( peer_id );

                if( known_peer == known_peers.end() ) {

                    known_peers[ peer_id ] = this;

                } else {

                    *(args -> stop) = true;
                }

                pthread_mutex_unlock( &known_peers_mutex );

                if( ! *(args -> stop) ) {

                    char* l_req = (char*)malloc( 1 );
                    l_req[ 0 ] = 'w';

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                        sizeof( *item ) );

                    item -> len = 1;
                    item -> cb = &Client::discovery_cb6;
                    item -> ctx = NULL;
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

            known_peers_t::iterator known_peer = known_peers.find( _peer_id );

            if( known_peer == known_peers.end() ) {

                if( strcmp( _peer_id, my_peer_id ) == 0 ) {

                    pthread_mutex_lock( &me_mutex );

                    me_t::iterator it = me.find( _peer_id );

                    if( it == me.end() ) {

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

                known_peers_t::iterator known_peer = known_peers.find( peer_id );

                if( known_peer != known_peers.end() )
                    known_peers.erase( known_peer );

                free( peer_id );
            }

            pthread_mutex_unlock( &known_peers_mutex );

            while( ! pending_reads.empty() ) {

                PendingReadsQueueItem** _item = pending_reads.front();
                PendingReadsQueueItem* item = *_item;

                free( item );
                free( _item );

                pending_reads.pop();
            }

            while( ! write_queue.empty() ) {

                WriteQueueItem* item = write_queue.front();
                write_queue.pop();

                free( item -> data );
                if( item -> cb != NULL ) free( item -> cb );
                free( item );
            }

            pthread_mutex_destroy( &write_queue_mutex );

            if( read_queue != NULL ) free( read_queue );
            if( peer_name != NULL ) free( peer_name );
            if( conn_name != NULL ) free( conn_name );
            if( conn_id != NULL ) free( conn_id );
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

        void push_pending_reads_queue( PendingReadsQueueItem* item ) {

            PendingReadsQueueItem** _item = (PendingReadsQueueItem**)malloc(
                sizeof( *_item ) );
            *_item = item;

            pending_reads.push( _item );
        }

        PendingReadsQueueItem* discovery_cb2( PendingReadCallbackArgs* args ) {

            if( args -> data[ 0 ] == 'k' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc(
                    sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::discovery_cb3;
                item -> ctx = NULL;
                item -> opcode = false;

                return item;

            } else {

                *(args -> stop) = true;

                return nullptr;
            }
        }
};

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

            int written = write( _watcher -> fd, ( item -> data + item -> pos ), item -> len );

            if( written < 0 ) {

                if( ( errno != EAGAIN ) && ( errno != EINTR ) ) {

                    perror( "write" );
                    delete client;

                    return;
                }

            } else {

                item -> pos += written;

                if( item -> cb != NULL ) {

                    client -> push_pending_reads_queue( item -> cb );
                    item -> cb = NULL;
                }
            }
        }
    }

    return;
}

void* client_thread( void* args ) {

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

void discovery_cb1( Client* client ) {

    char* w_req = (char*)malloc( 1 );
    w_req[ 0 ] = 'w';

    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

    item -> len = 1;
    item -> cb = &Client::discovery_cb2;
    item -> ctx = NULL;
    item -> opcode = true;

    client -> push_write_queue( 1, w_req, item );
}

void* discovery_thread( void* args ) {

    while( true ) {

        for(
            peers_to_discover_t::iterator it = peers_to_discover.begin();
            it != peers_to_discover.end();
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

                fprintf( stderr, "getaddrinfo: %s\n", gai_strerror( rv ) );
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

                    pthread_mutex_lock( &me_mutex );

                    me_t::iterator it = me.find( conn_id );

                    if( it != me.end() )
                        found = true;

                    pthread_mutex_unlock( &me_mutex );

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

void* synchronization_thread( void* args ) {

    while( true ) {

        sleep( 1 );
        db.synchronize();

        pthread_mutex_lock( &stat_mutex );

        printf( "number of inserts for last second: %llu\n", stat_num_inserts );
        stat_num_inserts = 0;

        pthread_mutex_unlock( &stat_mutex );
    }

    return NULL;
}

int main() {

    printf("k\n");
    signal( SIGPIPE, SIG_IGN );

    if( ! db.open(
        "/Users/gennadiy/kch/skree.kch",
        kyotocabinet::HashDB::OWRITER
        | kyotocabinet::HashDB::OCREATE
        | kyotocabinet::HashDB::ONOLOCK
        | kyotocabinet::HashDB::OAUTOTRAN
    ) ) {

        printf( "db.open: %s\n", db.error().name() );
        return 1;
    }

    my_hostname = (char*)"muhhost\0";
    my_hostname_len = strlen( my_hostname );
    my_port = 7654;
    my_peer_id = make_peer_id( my_hostname_len, my_hostname, my_port );

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

    std::queue<pthread_t*> threads;

    for( int i = 0; i < 10; ++i ) {

        pthread_t* thread = (pthread_t*)malloc( sizeof( *thread ) );

        pthread_create( thread, NULL, client_thread, NULL );

        threads.push( thread );
    }

    {
        peer_to_discover_t* localhost7654 = (peer_to_discover_t*)malloc(
            sizeof( *localhost7654 ) );

        localhost7654 -> host = "127.0.0.1\0";
        localhost7654 -> port = 7654;

        peer_to_discover_t* localhost8765 = (peer_to_discover_t*)malloc(
            sizeof( *localhost8765 ) );

        localhost8765 -> host = "127.0.0.1\0";
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

    pthread_t synchronization;
    pthread_create( &synchronization, NULL, synchronization_thread, NULL );

    pthread_t discovery;
    pthread_create( &discovery, NULL, discovery_thread, NULL );

    ev_run( loop, 0 );

    pthread_join( discovery, NULL );

    while( ! threads.empty() ) {

        pthread_t* thread = threads.front();
        threads.pop();

        pthread_join( *thread, NULL );
        free( thread );
    }

    pthread_join( synchronization, NULL );

    pthread_mutex_destroy( &known_peers_mutex );
    pthread_mutex_destroy( &new_clients_mutex );
    pthread_mutex_destroy( &me_mutex );
    pthread_mutex_destroy( &peers_to_discover_mutex );
    pthread_mutex_destroy( &stat_mutex );

    return 0;
}
