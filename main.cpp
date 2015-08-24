#include <ev.h>
#include <queue>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unordered_map>

size_t read_size = 131072;
time_t discovery_timeout_milliseconds = 3000;

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

std::unordered_map<char*, Client*, char_pointer_hasher, char_pointer_comparator> known_peers;
pthread_mutex_t known_peers_mutex;

struct peer_to_discover_t {

    uint32_t port;
    const char* host;
};

std::unordered_map<char*, peer_to_discover_t, char_pointer_hasher, char_pointer_comparator> peers_to_discover;

struct client_bound_ev_io {

    ev_io watcher;
    Client* client;
};

struct PendingReadCallbackArgs {

    char* data;
    size_t len;
    char** out_packet;
    size_t* out_packet_len;
};

struct PendingReadsQueueItem {

    size_t len;
    PendingReadsQueueItem* (Client::*cb) (PendingReadCallbackArgs*);
};

struct WriteQueueItem {

    size_t len;
    size_t pos;
    char* data;
    PendingReadsQueueItem* cb;
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
        char* peer_id;
        std::queue<PendingReadsQueueItem**> pending_reads;
        sockaddr_in* s_in;
        socklen_t s_in_len;

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

                    auto _item = pending_reads.front();
                    auto item = *_item;
                    --in_packet_len;

                    if( read_queue_length >= item -> len ) {

                        PendingReadCallbackArgs args = {
                            .data = read_queue + in_packet_len,
                            .len = item -> len,
                            .out_packet = &out_packet,
                            .out_packet_len = &out_packet_len
                        };

                        auto new_item = (this ->* (item -> cb))( &args );
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

                    } else {

                        // If we have a pending read, but requested amount
                        // of data is still not arrived - we should wait for it
                        break;
                    }

                } else if( opcode == 'w' ) {

                    const char* hostname = "muhhost\0";
                    uint32_t hostname_len = strlen( hostname );

                    out_packet = (char*)malloc( 1 + sizeof( hostname_len ) + hostname_len );

                    out_packet[ 0 ] = 'k';
                    out_packet_len += 1;

                    uint32_t _hostname_len = htonl( hostname_len );
                    memcpy( out_packet + out_packet_len, &_hostname_len, sizeof( hostname_len ) );
                    out_packet_len += sizeof( hostname_len );

                    memcpy( out_packet + out_packet_len, hostname, hostname_len );
                    out_packet_len += hostname_len;

                } else if( opcode == 'l' ) {

                    uint32_t _known_peers_len;
                    out_packet = (char*)malloc( 1 + sizeof( _known_peers_len ) );

                    out_packet[ 0 ] = 'k';
                    out_packet_len += 1;

                    pthread_mutex_lock( &known_peers_mutex );

                    _known_peers_len = htonl( known_peers.size() );
                    memcpy( out_packet + out_packet_len, &_known_peers_len,
                        sizeof( _known_peers_len ) );
                    out_packet_len += sizeof( _known_peers_len );

                    for( auto it = known_peers.begin(); it != known_peers.end(); ++it ) {

                        auto peer = it -> second;

                        auto peer_name_len = peer -> get_peer_name_len();
                        uint32_t _peer_name_len = htonl( peer_name_len );
                        uint32_t _peer_port = htonl( peer -> get_peer_port() );

                        out_packet = (char*)realloc( out_packet, out_packet_len
                            + sizeof( _peer_name_len ) + peer_name_len + sizeof( _peer_port ) );

                        memcpy( out_packet + out_packet_len, &_peer_name_len,
                                sizeof( _peer_name_len ) );
                        out_packet_len += sizeof( _peer_name_len );

                        memcpy( out_packet + out_packet_len, peer -> get_peer_name(),
                            peer_name_len );
                        out_packet_len += peer_name_len;

                        memcpy( out_packet + out_packet_len, &_peer_port, sizeof( _peer_port ) );
                        out_packet_len += sizeof( _peer_port );
                    }

                    pthread_mutex_unlock( &known_peers_mutex );

                } else if( opcode == 'e' ) {



                } else if( opcode == 'r' ) {



                } else if( opcode == 'c' ) {



                } else if( opcode == 'i' ) {



                } else if( opcode == 'x' ) {



                } else if( opcode == 'h' ) {

                    PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

                    item -> len = 4;
                    item -> cb = &Client::packet_h_cb1;

                    push_pending_reads_queue( item );

                } else {

                    printf( "Unknown packet header: 0x%.2X\n", opcode );
                }

                read_queue_length -= in_packet_len;
                if( read_queue_length > 0 )
                    memmove( read_queue, read_queue + in_packet_len, read_queue_length );

                if( out_packet_len > 0 )
                    push_write_queue( out_packet_len, out_packet, NULL );
            }
        }

        PendingReadsQueueItem* packet_h_cb1( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

            item -> len = len + 4;
            item -> cb = &Client::packet_h_cb2;

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

            auto _peer_id = make_peer_id( host_len, host, port );

            pthread_mutex_lock( &known_peers_mutex );

            auto known_peer = known_peers.find( _peer_id );

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

        PendingReadsQueueItem* discovery_cb4( PendingReadCallbackArgs* args ) {

            auto peer_id = make_peer_id( args -> len, args -> data, this -> get_conn_port() );

            printf( "ERMAHGERD DERSCERVEREY: %s\n", peer_id );

            return nullptr;
        }

        PendingReadsQueueItem* discovery_cb3( PendingReadCallbackArgs* args ) {

            uint32_t _len;
            memcpy( &_len, args -> data, args -> len );
            uint32_t len = ntohl( _len );

            PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

            item -> len = len;
            item -> cb = &Client::discovery_cb4;

            return item;
        }

    public:
        Client( int _fh, struct ev_loop* _loop, sockaddr_in* _s_in, socklen_t _s_in_len )
            : fh( _fh ), loop( _loop ), s_in( _s_in ), s_in_len( _s_in_len ) {

            read_queue = NULL;
            peer_name = NULL;
            peer_id = NULL;
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

                auto known_peer = known_peers.find( peer_id );

                if( known_peer != known_peers.end() )
                    known_peers.erase( known_peer );

                free( peer_id );
            }

            pthread_mutex_unlock( &known_peers_mutex );

            while( ! pending_reads.empty() ) {

                auto _item = pending_reads.front();
                auto item = *_item;

                free( item );
                free( _item );

                pending_reads.pop();
            }

            while( ! write_queue.empty() ) {

                auto item = write_queue.front();
                write_queue.pop();

                free( item -> data );
                if( item -> cb != NULL ) free( item -> cb );
                free( item );
            }

            pthread_mutex_destroy( &write_queue_mutex );

            if( read_queue != NULL ) free( read_queue );
            if( peer_name != NULL ) free( peer_name );
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

            if( s_in -> sin_family == AF_INET ) {

                return ntohs( s_in -> sin_port );

            } else {

                return ntohs( ((sockaddr_in6*)s_in) -> sin6_port );
            }
        }

        char* get_peer_id() {

            if( peer_id != NULL )
                return peer_id;

            if( peer_name == NULL )
                return NULL;

            peer_id = make_peer_id( peer_name_len, peer_name, peer_port );

            return peer_id;
        }

        void push_read_queue( size_t len, char* data ) {

            auto offset = read_queue_length;

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

                auto item = write_queue.front();

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

            PendingReadsQueueItem** _item = (PendingReadsQueueItem**)malloc( sizeof( *_item ) );
            *_item = item;

            pending_reads.push( _item );
        }

        PendingReadsQueueItem* discovery_cb2( PendingReadCallbackArgs* args ) {

            if( args -> data[ 0 ] == 'k' ) {

                PendingReadsQueueItem* item = (PendingReadsQueueItem*)malloc( sizeof( *item ) );

                item -> len = 4;
                item -> cb = &Client::discovery_cb3;

                return item;

            } else {

                delete this;

                return nullptr;
            }
        }
};

static void client_cb( struct ev_loop* loop, ev_io* _watcher, int events ) {

    struct client_bound_ev_io* watcher = (struct client_bound_ev_io*)_watcher;
    auto client = watcher -> client;

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

        auto item = client -> get_pending_write();

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

                auto new_client = new_clients.front();
                new_clients.pop();

                pthread_mutex_unlock( &new_clients_mutex );

                auto client = new Client(
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

    client -> push_write_queue( 1, w_req, item );
}

void* discovery_thread( void* args ) {

    while( true ) {

        for( auto it = peers_to_discover.begin(); it != peers_to_discover.end(); ++it ) {

            printf( "about to discovery %s...\n", it -> first );
            auto peer_to_discover = it -> second;

            addrinfo hints;
            addrinfo* service_info;

            memset( &hints, 0, sizeof( hints ) );
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_flags = AI_NUMERICSERV;

            char port[ 6 ];
            sprintf( port, "%d", peer_to_discover.port );

            int rv;

            if( ( rv = getaddrinfo( peer_to_discover.host, port, &hints, &service_info ) ) != 0 ) {

                fprintf( stderr, "getaddrinfo: %s\n", gai_strerror( rv ) );
                continue;
            }

            int fh;
            int yes = 1;
            sockaddr_in* addr = (sockaddr_in*)malloc( sizeof( *addr ) );
            socklen_t addr_len;
            bool connected = false;

            for( auto ai_it = service_info; ai_it != NULL; ai_it = ai_it -> ai_next ) {

                if( ( fh = socket( ai_it -> ai_family, ai_it -> ai_socktype, ai_it -> ai_protocol ) ) == -1 ) {

                    perror( "socket" );
                    continue;
                }

                if( setsockopt( fh, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof( yes ) ) == -1 ) {

                    perror( "setsockopt" );
                    continue;
                }

                if( setsockopt( fh, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof( yes ) ) == -1 ) {

                    perror( "setsockopt" );
                    continue;
                }

                timeval tv;
                tv.tv_sec = ( discovery_timeout_milliseconds / 1000 );
                tv.tv_usec = ( ( discovery_timeout_milliseconds % 1000 ) * 1000 );

                if( setsockopt( fh, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof( tv ) ) == -1 ) {

                    perror( "setsockopt" );
                    continue;
                }

                if( setsockopt( fh, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof( tv ) ) == -1 ) {

                    perror( "setsockopt" );
                    continue;
                }

                if( connect( fh, ai_it -> ai_addr, ai_it -> ai_addrlen ) == -1 ) {

                    if( errno != EINPROGRESS ) {

                        close( fh );
                        perror( "connect" );
                        continue;
                    }

                    fcntl( fh, F_SETFL, fcntl( fh, F_GETFL, 0 ) | O_NONBLOCK );
                }

                *addr = *(sockaddr_in*)(ai_it -> ai_addr);
                addr_len = ai_it -> ai_addrlen;
                connected = true;

                break;
            }

            if( connected ) {

                if( getpeername( fh, (sockaddr*)addr, &addr_len ) == -1 ) {

                    perror( "getpeername" );
                    close( fh );
                    free( addr );

                } else {

                    new_client_t* new_client = (new_client_t*)malloc( sizeof( *new_client ) );

                    new_client -> fh = fh;
                    new_client -> cb = discovery_cb1;
                    new_client -> s_in = addr;
                    new_client -> s_in_len = addr_len;

                    pthread_mutex_lock( &new_clients_mutex );
                    new_clients.push( new_client );
                    pthread_mutex_unlock( &new_clients_mutex );
                }

            } else {

                fprintf( stderr, "Peer %s is unreachable\n", it -> first );
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

int main() {

    printf("k\n");
    signal( SIGPIPE, SIG_IGN );

    sockaddr_in addr;

    int fh = socket( PF_INET, SOCK_STREAM, 0 );

    addr.sin_family = AF_UNSPEC;
    addr.sin_port = htons( 7654 );
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

    std::queue<pthread_t*> threads;

    for( int i = 0; i < 1; ++i ) {

        pthread_t* thread = (pthread_t*)malloc( sizeof( *thread ) );

        pthread_create( thread, NULL, client_thread, NULL );

        threads.push( thread );
    }

    {
        peer_to_discover_t localhost7654 = {
            .port = 7654,
            .host = "127.0.0.1\0"
        };

        peer_to_discover_t localhost8765 = {
            .port = 8765,
            .host = "127.0.0.1\0"
        };

        peers_to_discover[ make_peer_id(
            strlen( localhost7654.host ),
            (char*)localhost7654.host,
            localhost7654.port

        ) ] = localhost7654;

        peers_to_discover[ make_peer_id(
            strlen( localhost8765.host ),
            (char*)localhost8765.host,
            localhost8765.port

        ) ] = localhost8765;
    }

    pthread_t discovery;
    pthread_create( &discovery, NULL, discovery_thread, NULL );

    ev_run( loop, 0 );

    pthread_join( discovery, NULL );

    while( ! threads.empty() ) {

        auto thread = threads.front();
        threads.pop();

        pthread_join( *thread, NULL );
        free( thread );
    }

    pthread_mutex_destroy( &known_peers_mutex );
    pthread_mutex_destroy( &new_clients_mutex );

    return 0;
}
