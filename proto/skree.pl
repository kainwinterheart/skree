#!/usr/bin/perl -w

package asd;

use strict;
use warnings;
use boolean;

use EV ();
use POSIX 'SIGTERM';
use Config '%Config';
use Socket 'AF_INET', 'AF_INET6';
use AnyEvent ();
use Getopt::Long 'GetOptions';
use KyotoCabinet ();
use Scalar::Util 'weaken';
use Salvation::TC ();
use AnyEvent::Handle ();
use AnyEvent::Socket 'tcp_server', 'tcp_connect';
use Sys::Hostname::FQDN 'fqdn';
use Salvation::DaemonDecl;
use Salvation::Method::Signatures;

use constant {

    NO_FAILOVER_TIME => 10 * 60,
    UNPACK_SOCKADDR_MAP => {
        AF_INET() => \&Socket::unpack_sockaddr_in,
        AF_INET6() => \&Socket::unpack_sockaddr_in6,
    },
};

my $kch = undef;
my $ipv6 = false;
my $listen = true;
my $job_time = 10 * 60;
my $repl_factor = 2;
my $server_port = undef;
my $max_processors = 10;
my $repl_check_time = 30 * 60;

GetOptions(
    'port=i' => \$server_port,
    'ipv6!' => \$ipv6,
    'listen!' => \$listen,
    'workers=i' => \$max_processors,
    'kch=s' => \$kch,
    'repl-factor=i' => \$repl_factor,
    'repl-check-time=i' => \$repl_check_time,
    'job-time=i' => \$job_time,
);

Salvation::TC -> assert( [ $kch ], 'ArrayRef[Str]' );

my %me = ();
my $fqdn = ( fqdn() // '' );
my @peers = ();
my %peers = ();
my @servers = ();
my $master_pid = undef;
my %main_accept_cb = ();
my %variable_accept = ();
my %peers_by_conn_addr = ();

method repl_save(
    KyotoCabinet::DB :db!, Str{1,} :event!, Str{1,} :hostname!, Int :peer_port!,
    HashRef :node!, Str{1,} :data!, Int :id!, Maybe[Str] :replicas!
) {

    my $suffix = "${event}:${hostname}:${peer_port}";
    my $current_in_seq = $db -> increment(
        "rinseq:${suffix}", 1, 0 );

    $node -> { 'current_in_seq' } = $current_in_seq;

    $db -> set_bulk( {
        sprintf( 'rin:%s:%d', $suffix, ( $current_in_seq - 1 ) ) => $data,
        sprintf( 'rts:%s:%d', $suffix, ( $current_in_seq - 1 ) ) => time(),
        sprintf( 'rid:%s:%d', $suffix, ( $current_in_seq - 1 ) ) => $id,
        sprintf( 'rre:%s:%d', $suffix, $id ) => ( $current_in_seq - 1 ),
        ( ( length( $replicas // '' ) > 0 ) ? (
            sprintf( 'rpr:%s:%d', $suffix, ( $current_in_seq - 1 ) ) => $replicas,
        ) : () ),
    } );

    return;
}

method save_event(
    KyotoCabinet::DB :db!, HashRef :node!, Str{1,} :event!, Str{1,} :data!
) {

    my $current_in_seq = $db -> increment(
        "inseq:${event}", 1, 0 );

    $node -> { 'current_in_seq' } = $current_in_seq;

    $db -> set(
        sprintf(
            'in:%s:%d',
            $event,
            ( $current_in_seq - 1 ),
        ),
        $data,
    );

    if( $repl_factor > 0 ) {

        my $i = 0;
        my %seen = ();
        my @hosts = ();
        my @stack = ();
        my $packet = 'r' . pack( 'N', length( $fqdn ) )
            . $fqdn . pack( 'N', $server_port )
            . pack( 'N', length( $event ) )
            . $event . pack( 'Q', ( $current_in_seq - 1 ) )
            . pack( 'N', length( $data ) ) . $data;

        # warn 'about to replicate...';

        foreach my $peer ( sort( { rand() <=> rand() } @peers ) ) {

            # warn $peer -> { 'hostname' } . ':' . $peer -> { 'port' } . '...';

            if( exists $peers{ $peer -> { 'hostname' } }
                -> { $peer -> { 'port' } } ) {

                # warn '...looks good';

                my $ok = false;

                $peer -> { 'on_error' } -> ( sub {

                    return if $ok;
                    return if $$ != $master_pid;

                    if( defined( my $next = shift( @stack ) ) ) {

                        $next -> ();

                    } else {

                        undef( @stack );
                    }
                } );

                push( @stack, sub {

                    # warn 'requesting ' . $peer -> { 'hostname' } . ':' . $peer -> { 'port' } . ' to accept replication...';

                    my $packet = $packet . pack( 'N', scalar( @hosts ) );

                    foreach my $host ( @hosts ) {

                        $packet .=
                            pack( 'N', length( $host -> { 'host' } ) )
                            . $host -> { 'host' }
                            . pack( 'N', $host -> { 'port' } )
                        ;
                    }

                    $peer -> { 'fh' } -> push_write( $packet );

                    # warn 'variable_accept[' . $peer -> { 'host' } . ':' . $peer -> { 'conn_port' } . '].push';

                    push( @{ $variable_accept{
                        $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                    } }, {
                        k => sub {

                            return if $$ != $master_pid;

                            my ( $client ) = @_;
                            $ok = true;
    # warn 'ro:k';
                            push( @hosts, {
                                host => $peer -> { 'hostname' },
                                port => $peer -> { 'port' },
                            } );

                            if( ++$i >= $repl_factor ) {

                                undef( @stack );

                                $peer -> { 'fh' } -> push_read( chunk => 1, $main_accept_cb{
                                    $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                } ) if defined $peer -> { 'fh' };

                            } elsif( defined( my $next = shift( @stack ) ) ) {

                                $next -> ();

                            } else {

                                undef( @stack );

                                $peer -> { 'fh' } -> push_read( chunk => 1, $main_accept_cb{
                                    $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                } ) if defined $peer -> { 'fh' };
                            }
                        },

                        f => sub {

                            return if $$ != $master_pid;

                            my ( $client ) = @_;
                            $ok = true;

    # warn 'ro:f: ', $data, ' (', $peer -> { 'hostname' }, ':', $peer -> { 'port' }, ')';
                            if( defined( my $next = shift( @stack ) ) ) {

                                $next -> ();

                            } else {

                                undef( @stack );

                                $peer -> { 'fh' } -> push_read( chunk => 1, $main_accept_cb{
                                    $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                } ) if defined $peer -> { 'fh' };
                            }
                        },
                    } );

                    $peer -> { 'fh' } -> push_read( chunk => 1, $main_accept_cb{
                        $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                    } ) if defined $peer -> { 'fh' };
                } );

            } else {

                # warn '...have no link';
            }
        }

        if( defined( my $code = shift( @stack ) ) ) {

            $code -> ();
        }
    }

    return ( $current_in_seq - 1 );
}

worker {
    name 'processor',
    max_instances $max_processors,
    wo,
    log {
        warn @_;
    },
    main {
        undef( @servers );
        my ( $worker, $event, $data ) = @_;

        warn "${event}: ${data}";

        $worker -> write_to_parent( 'k' );
        wait_cond( AnyEvent -> condvar() );

        return;
    },
};

worker {
    name 'main',
    max_instances 1,
    log {
        warn @_;
    },
    reap {
        my ( $finish, $pid, $status ) = @_;

        warn "${pid}: ", ( split( /\s+/, $Config{ 'sig_name' } ) )[ $status ];

        $finish -> ();
    },
    main {
        my $db = KyotoCabinet::DB -> new();

        unless( $db -> open( $kch, (
            $db -> OWRITER() | $db -> OCREATE() | $db -> ONOLOCK() | $db -> OAUTOSYNC()
        ) ) ) {

            die( "open(${kch}): " . $db -> error() );
        }

        my %wip = ();
        my %events = ();
        my %clients = ();
        my %failover = ();
        $master_pid = $$;
        my %hosts_list = ();
        my %no_failover = ();
        my %replication = ();

        my $accept = sub {

            my ( $client, %args ) = @_;
            my ( $host, $port ) = @$client{ 'host', 'conn_port' };
            # warn "accept: ${host}:${port}";
            no warnings 'syntax';
            my ( $do_not_read ) = @args{ 'do_not_read' };
            use warnings 'syntax';
            my $cb = undef;
            my $fh = $client -> { 'fh' };
            my $read_queue = $variable_accept{ "${host}:${port}" } //= [];

            my $weak_cb;
            $cb = sub {

                return if $$ != $master_pid;

                my ( undef, $data ) = @_;
# warn "${host}:${port} ${data}";
                if( $data eq 'w' ) {

                    $fh -> push_write( 'k' . pack( 'N', length( $fqdn ) ) . $fqdn );
                    $fh -> push_read( chunk => 1, $weak_cb );

                } elsif( $data eq 'l' ) {

                    my $packet = 'k' . pack( 'N', scalar( @peers ) );

                    foreach my $peer ( @peers ) {

                        $packet .=
                            pack( 'N', length( $peer -> { 'host' } ) )
                            . $peer -> { 'host' }
                            . pack( 'N', $peer -> { 'port' } )
                        ;
                    }

                    $fh -> push_write( $packet );
                    $fh -> push_read( chunk => 1, $weak_cb );

                } elsif( $data eq 'e' ) {

                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;

                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 4, sub {

                            return if $$ != $master_pid;

                            my ( undef, $event ) = @_;
                            $length = substr( $event, $length, 4, '' );
                            $length = unpack( 'N', $length );

                            $fh -> push_read( chunk => $length, sub {

                                return if $$ != $master_pid;

                                my ( undef, $data ) = @_;
                                my $rv = $db -> transaction( sub {

                                    asd -> save_event(
                                        db => $db,
                                        data => $data,
                                        node => $events{ $event } //= {},
                                        event => $event,
                                    );

                                    1;
                                } );

                                unless( $rv ) {

                                    die( 'save_event(): ' . $db -> error() );
                                }

                                $fh -> push_write( 'k' );
                                $fh -> push_read( chunk => 1, $weak_cb );
                            } );
                        } );
                    } );

                } elsif( $data eq 'r' ) {

                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;
# warn 1;
                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 8, sub {

                            return if $$ != $master_pid;
# warn 2;
                            my ( undef, $hostname ) = @_;
                            my $peer_port = substr( $hostname, $length, 4, '' );
                            $peer_port = unpack( 'N', $peer_port );
                            $length = substr( $hostname, $length, 4, '' );
                            $length = unpack( 'N', $length );

                            $fh -> push_read( chunk => $length + 12, sub {

                                return if $$ != $master_pid;
# warn 3;
                                my ( undef, $event ) = @_;
                                my $id = substr( $event, $length, 8, '' );
                                $id = unpack( 'Q', $id );
                                $length = substr( $event, $length, 4, '' );
                                $length = unpack( 'N', $length );

                                $fh -> push_read( chunk => $length + 4, sub {

                                    return if $$ != $master_pid;
# warn 4;
                                    my ( undef, $data ) = @_;
                                    my @stack = ();
                                    my $replicas = '';
                                    $length = substr( $data, $length, 4, '' );
                                    $length = unpack( 'N', $length );

                                    foreach ( 1 .. $length ) {

                                        push( @stack, sub {

                                            $fh -> push_read( chunk => 4, sub {

                                                return if $$ != $master_pid;
# warn "5.$_";
                                                my ( undef, $length ) = @_;
                                                $length = unpack( 'N', $length );

                                                $fh -> push_read( chunk => $length + 4, sub {

                                                    my ( undef, $hostname ) = @_;

                                                    $replicas .=
                                                        pack( 'N', $length )
                                                        . $hostname
                                                    ;
                                                    shift( @stack ) -> ();
                                                } );
                                            } );
                                        } );
                                    }

                                    push( @stack, sub {
# warn 6;
                                        if(
                                            ( $hostname eq $fqdn )
                                            && ( $server_port == $peer_port )
                                        ) {
# warn 'r:f';
                                            $fh -> push_write( 'f' );

                                        } else {

                                            my $rv = $db -> transaction( sub {

                                                asd -> repl_save(
                                                    db => $db,
                                                    id => $id,
                                                    data => $data,
                                                    node => $replication{ $event }
                                                        -> { $hostname }
                                                        -> { $peer_port } //= {},
                                                    event => $event,
                                                    hostname => $hostname,
                                                    replicas => $replicas,
                                                    peer_port => $peer_port,
                                                );

                                                1;
                                            } );

                                            unless( $rv ) {

                                                die( 'repl_save(): ' . $db -> error() );
                                            }
# warn 'r:k';
                                            $fh -> push_write( 'k' );
                                        }

                                        $fh -> push_read( chunk => 1, $weak_cb );
                                    } );

                                    shift( @stack ) -> ();
                                } );
                            } );
                        } );
                    } );

                } elsif( $data eq 'c' ) {

                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;

                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 12, sub {

                            return if $$ != $master_pid;

                            my ( undef, $event ) = @_;
                            my $id = substr( $event, $length, 8, '' );
                            $id = unpack( 'Q', $id );
                            $length = substr( $event, $length, 4, '' );
                            $length = unpack( 'N', $length );

                            $fh -> push_read( chunk => $length, sub {

                                return if $$ != $master_pid;

                                my ( undef, $data ) = @_;

                                if( exists $wip{ $id } ) {

                                    if( ( $wip{ $id } + $job_time ) <= time() ) {

                                        my $rv = $db -> transaction( sub {

                                            asd -> save_event(
                                                db => $db,
                                                data => $data,
                                                node => $events{ $event } //= {},
                                                event => $event,
                                            );

                                            1;
                                        } );

                                        unless( $rv ) {

                                            die( 'save_event(): ' . $db -> error() );
                                        }

                                        $rv = $db -> remove( "in:${event}:${id}" );

                                        unless( $rv ) {

                                            warn( 'remove(): ' . $db -> error() );
                                        }

                                        $fh -> push_write( 'k' );
                                        delete( $wip{ $id } );

                                    } else {

                                        $fh -> push_write( 'f' );
                                    }

                                } else {

                                    my $rv = $db -> check( "in:${event}:${id}" );

                                    if( ( $rv // 0 ) > 0 ) {

                                        my $rv = $db -> transaction( sub {

                                            asd -> save_event(
                                                db => $db,
                                                data => $data,
                                                node => $events{ $event } //= {},
                                                event => $event,
                                            );

                                            1;
                                        } );

                                        unless( $rv ) {

                                            die( 'save_event(): ' . $db -> error() );
                                        }

                                        $rv = $db -> remove( "in:${event}:${id}" );

                                        unless( $rv ) {

                                            warn( 'remove(): ' . $db -> error() );
                                        }
                                    }

                                    $fh -> push_write( 'k' );
                                }

                                $fh -> push_read( chunk => 1, $weak_cb );
                            } );
                        } );
                    } );

                } elsif( $data eq 'i' ) {

                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;

                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 8, sub {

                            return if $$ != $master_pid;

                            my ( undef, $hostname ) = @_;
                            my $peer_port = substr( $hostname, $length, 4, '' );
                            $peer_port = unpack( 'N', $peer_port );
                            $length = substr( $hostname, $length, 4, '' );
                            $length = unpack( 'N', $length );

                            $fh -> push_read( chunk => $length + 8, sub {

                                return if $$ != $master_pid;

                                my ( undef, $event ) = @_;
                                my $id = substr( $event, $length, 8, '' );
                                $id = unpack( 'N', $id );

                                if( exists $failover{ $event } -> { $hostname }
                                    -> { $peer_port } -> { $id } ) {

                                    if( exists $wip{ $failover{ $event }
                                        -> { $hostname } -> { $peer_port } -> { $id } } ) {

                                        $fh -> push_write( 'f' );

                                    } else {

                                        if( int( rand( 100 ) + 0.5 ) > 50 ) {

                                            $fh -> push_write( 'f' );

                                        } else {

                                            $fh -> push_write( 'k' );

                                            delete( $failover{ $event } -> { $hostname }
                                                -> { $peer_port } -> { $id } );

                                            $no_failover{ $event } -> { $hostname }
                                                -> { $peer_port } -> { $id } = time();
                                        }
                                    }

                                } else {

                                    my $suffix = "${event}:${hostname}:${peer_port}";
                                    my $my_id = $db -> get( "rre:${suffix}:${id}" );

                                    if( defined $my_id ) {

                                        $fh -> push_write( 'k' );

                                    } else {

                                        $fh -> push_write( 'f' );
                                    }
                                }

                                $fh -> push_read( chunk => 1, $weak_cb );
                            } );
                        } );
                    } );

                } elsif( $data eq 'x' ) {

                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;

                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 8, sub {

                            return if $$ != $master_pid;

                            my ( undef, $hostname ) = @_;
                            my $peer_port = substr( $hostname, $length, 4, '' );
                            $peer_port = unpack( 'N', $peer_port );
                            $length = substr( $hostname, $length, 4, '' );
                            $length = unpack( 'N', $length );

                            $fh -> push_read( chunk => $length + 8, sub {

                                return if $$ != $master_pid;

                                my ( undef, $event ) = @_;
                                my $id = substr( $event, $length, 8, '' );
                                $id = unpack( 'N', $id );

                                my $suffix = "${event}:${hostname}:${peer_port}";
                                my $my_id = $db -> get( "rre:${suffix}:${id}" );

                                if( defined $my_id ) {

                                    my $rv = $db -> remove_bulk( [
                                        "rin:${suffix}:${my_id}",
                                        "rts:${suffix}:${my_id}",
                                        "rid:${suffix}:${my_id}",
                                        "rre:${suffix}:${id}",
                                        "rpr:${suffix}:${my_id}",
                                    ] );

                                    unless( $rv ) {

                                        warn( 'remove_bulk(): ' . $db -> error() );
                                    }
                                }

                                $fh -> push_read( chunk => 1, $weak_cb );
                            } );
                        } );
                    } );

                } elsif( $data eq 'h' ) {
# warn $data;
                    $fh -> push_read( chunk => 4, sub {

                        return if $$ != $master_pid;

                        my ( undef, $length ) = @_;
                        $length = unpack( 'N', $length );

                        $fh -> push_read( chunk => $length + 4, sub {

                            return if $$ != $master_pid;

                            my ( undef, $hostname ) = @_;
                            my $peer_port = substr( $hostname, $length, 4, '' );
                            $peer_port = unpack( 'N', $peer_port );

                            $hosts_list{ $host } -> { $peer_port } = 1;

                            if( exists $peers{ $hostname } -> { $peer_port } ) {

                                $fh -> push_write( 'f' );

                            } else {

                                $client -> { 'on_error' } -> ( sub {

                                    undef( $fh );
                                    delete( $peers{ $hostname } -> { $peer_port } );
                                    delete( $peers_by_conn_addr{ "${host}:${port}" } );
                                    delete( $variable_accept{ "${host}:${port}" } );
                                } );

                                my $peer = {
                                    host => $host,
                                    port => $peer_port,
                                    fqdn => $hostname,
                                    hostname => $hostname,
                                    conn_port => $port,
                                    fh => $fh,
                                    on_error => $client -> { 'on_error' },
                                };

                                $peers{ $hostname } -> { $peer_port } = $peer;
                                $peers_by_conn_addr{ "${host}:${port}" } = $peer;
                                @peers = ( grep( {
                                    ! (
                                        ( $_ -> { 'hostname' } eq $hostname )
                                        && ( $_ -> { 'port' } eq $peer_port )
                                    );
                                } @peers ), $peer );

                                $fh -> push_write( 'k' );
                            }

                            $fh -> push_read( chunk => 1, $weak_cb );
                        } );
                    } );

                } elsif(
                    ( scalar( @$read_queue ) > 0 )
                    && exists $read_queue -> [ 0 ] -> { $data }
                ) {
# warn "variable_accept: ${host}:${port}";
                    shift( @$read_queue ) -> { $data } -> ( $client );

                } else {

                    warn 'Unknown packet header: 0x' . uc( sprintf( '%.2x', ord( $data ) ) );
                    $fh -> push_read( chunk => 1, $weak_cb );
                }
            };

            $client -> { 'on_error' } -> ( sub {

                undef( $cb );
            } );

            weaken( $weak_cb = $cb );
            $main_accept_cb{ "${host}:${port}" } = $weak_cb;

            $fh -> push_read( chunk => 1, $cb ) unless $do_not_read;

            return;
        };

        if( $listen ) {

            my $server_ip = ( $ipv6 ? '::' : '0' );

            Salvation::TC -> assert( $server_port, 'Int' );

            push( @servers, tcp_server( $server_ip, $server_port, sub {

                return if $$ != $master_pid;

                my ( $fh, $host, $port ) = @_;
                my $error = false;
                my $fileno = $fh -> fileno();
                my @on_error = ();
                my $client = $clients{ $fileno } = {
                    fh => AnyEvent::Handle -> new(
                        fh => $fh,
                        on_error => sub {
                            $error = true;
                            my @args = @_;

                            eval { $fh -> close() };
                            delete( $clients{ $fileno } );

                            while( defined( my $cb = shift( @on_error ) ) ) {

                                $cb -> ( @args );
                            }
                        },
                    ),
                    host => $host,
                    port => $port,
                    conn_port => $port,
                    on_error => sub {
                        my ( $cb ) = @_;

                        if( $error ) {

                            $cb -> ();

                        } else {

                            push( @on_error, $cb );
                        }
                    },
                };

                $accept -> ( $client );
            } ) );
        }

        foreach my $spec (
            [ 'ppcmoddev1', 8036 ],
            [ 'ppcmoddev2', 9033 ],
            [ '127.0.0.1', 7654 ],
            [ '127.0.0.1', 8765 ],
        ) {

            $hosts_list{ $spec -> [ 0 ] } -> { $spec -> [ 1 ] } = 1;
        }

        my $discovery_timer = AnyEvent -> timer( after => ( int( rand( 7 ) + 0.5 ) + 1 ), interval => 30, cb => sub {

            my @hosts_list = ();
            my %hosts_list = %hosts_list;

            while( my ( $host, $data ) = each( %hosts_list ) ) {

                while( my ( $port ) = each( %$data ) ) {

                    push( @hosts_list, [ $host, $port ] );
                }
            }

            my %seen = ();

            foreach my $spec ( sort( { rand() <=> rand() } @hosts_list ) ) {

                EV::run EV::RUN_NOWAIT;

                my ( $host, $port ) = @$spec;
                my @stack = ();
                my $already_discovered = false;

                foreach my $ai ( Socket::getaddrinfo( $host, $port ) ) {

                    next unless Salvation::TC -> is( $ai, 'HashRef( Int :family!, Str{1,} :addr! )' );

                    if( defined( my $code = UNPACK_SOCKADDR_MAP -> { $ai -> { 'family' } } ) ) {

                        my $host = eval{ Socket::inet_ntop(
                            $ai -> { 'family' },
                            ( $code -> ( $ai -> { 'addr' } ) )[ 1 ],
                        ) };

                        warn $@ if $@;

                        next unless defined $host;
                        next if $seen{ "${host}:${port}" } ++;

                        if(
                            $peers_by_conn_addr{ "${host}:${port}" }
                            || exists $me{ $host } -> { $port }
                            || exists $variable_accept{ "${host}:${port}" }
                        ) {

                            $already_discovered = true;
                            undef( @stack );
                            last;
                        }

                        push( @stack, sub {

                            return if $already_discovered;

                            my $guard; $guard = tcp_connect( $host, $port, sub {
            # warn "conn: ${host}:${port}";
                                return if $$ != $master_pid;

                                my ( $fh, $host ) = @_;

                                unless( $fh ) {
            # warn 'no connection';
                                    undef( $guard );

                                    if( defined( my $next = shift( @stack ) ) ) {

                                        $next -> ();

                                    } else {

                                        undef( @stack );
                                    }

                                    return;
                                }

                                my @on_error = ();

                                $fh = AnyEvent::Handle -> new(
                                    fh => $fh,
                                    on_error => sub {
                                        my @args = @_;

                                        while( defined( my $cb = shift( @on_error ) ) ) {

                                            $cb -> ( @args );
                                        }
                                    },
                                );

                                push( @on_error, sub {

                                    undef( $fh );
                                    undef( $guard );

                                    if( defined( my $next = shift( @stack ) ) ) {

                                        $next -> ();

                                    } else {

                                        undef( @stack );
                                    }
                                } );

                                my $error = false;
                                my $peer = {
                                    host => $host,
                                    port => $port,
                                    conn_port => $port,
                                    fh => $fh,
                                    on_error => sub {
                                        my ( $cb ) = @_;

                                        if( $error ) {

                                            $cb -> ();

                                        } else {

                                            push( @on_error, $cb );
                                        }
                                    },
                                };

                                $accept -> ( $peer, do_not_read => 1 );

                                $fh -> push_write( 'w' );
            # warn "w: ${host}:${port} ?";
                                push( @{ $variable_accept{ "${host}:${port}" } }, {

                                    k => sub {
            # warn "w: ${host}:${port} k";
                                        return if $$ != $master_pid;

                                        $fh -> push_read( chunk => 4, sub {

                                            return if $$ != $master_pid;

                                            my ( undef, $length ) = @_;
                                            $length = unpack( 'N', $length );

                                            $fh -> push_read( chunk => $length, sub {

                                                my ( undef, $hostname ) = @_;

                                                if(
                                                    ( $hostname eq $fqdn )
                                                    && ( $port == $server_port )
                                                ) {

                                                    undef( $fh );
                                                    undef( $guard );
                                                    undef( @stack );
                                                    $me{ $host } -> { $port } = 1;

                                                } else {

                                                    $fh -> push_write(
                                                        'h'
                                                        . pack( 'N', length( $fqdn ) )
                                                        . $fqdn . pack( 'N', $server_port )
                                                    );

                                                    push( @{ $variable_accept{ "${host}:${port}" } }, {

                                                        k => sub {
            # warn "h: ${host}:${port} k";
                                                            return if $$ != $master_pid;

                                                            undef( @stack );
                                                            my $keep_connection = false;

                                                            @on_error = ( sub {

                                                                $error = true;
                                                                undef( $fh );
                                                                undef( $guard );

                                                                if( $keep_connection ) {

                                                                    delete( $peers{ $hostname } -> { $port } );
                                                                    delete( $peers_by_conn_addr{ "${host}:${port}" } );
                                                                    delete( $variable_accept{ "${host}:${port}" } );
                                                                }
                                                            } );

                                                            @$peer{ 'fqdn', 'hostname' } = ( $hostname )x2;

                                                            unless( exists $peers{ $hostname } -> { $port } ) {

                                                                $peers{ $hostname } -> { $port } = $peer;
                                                                $peers_by_conn_addr{ "${host}:${port}" } = $peer;
                                                                @peers = ( grep( {
                                                                    ! (
                                                                        ( $_ -> { 'hostname' } eq $hostname )
                                                                        && ( $_ -> { 'port' } eq $port )
                                                                    );
                                                                } @peers ), $peer );

                                                                $keep_connection = true;
                                                            }

                                                            $fh -> push_write( 'l' );

                                                            push( @{ $variable_accept{ "${host}:${port}" } }, {

                                                                k => sub {

                                                                    return if $$ != $master_pid;

                                                                    $fh -> push_read( chunk => 4, sub {

                                                                        return if $$ != $master_pid;

                                                                        my ( undef, $length ) = @_;
                                                                        $length = unpack( 'N', $length );
                                                                        my @stack = ();

                                                                        if( $length > 0 ) {

                                                                            $peer -> { 'on_error' } -> ( sub {

                                                                                undef( @stack );
                                                                            } );
                                                                        }

                                                                        foreach ( 1 .. $length ) {

                                                                            push( @stack, sub {

                                                                                $fh -> push_read( chunk => 4, sub {

                                                                                    return if $$ != $master_pid;

                                                                                    my ( undef, $length ) = @_;
                                                                                    $length = unpack( 'N', $length );

                                                                                    $fh -> push_read( chunk => $length + 4, sub {

                                                                                        return if $$ != $master_pid;

                                                                                        my ( undef, $hostname ) = @_;
                                                                                        my $peer_port = substr( $hostname, $length, 4, '' );
                                                                                        $peer_port = unpack( 'N', $peer_port );

                                                                                        $hosts_list{ $hostname } -> { $peer_port } = 1;

                                                                                        if( defined( my $next = shift( @stack ) ) ) {

                                                                                            $next -> ();
                                                                                        }
                                                                                    } );
                                                                                } );
                                                                            } );
                                                                        }

                                                                        push( @stack, sub {

                                                                            unless( $keep_connection ) {

                                                                                undef( $fh );
                                                                                undef( $guard );
                                                                            }

                                                                            undef( @stack );

                                                                            $fh -> push_read( chunk => 1, $main_accept_cb{
                                                                                "${host}:${port}"
                                                                            } ) if defined $fh;
                                                                        } );

                                                                        shift( @stack ) -> ();
                                                                    } );
                                                                },
                                                            } );

                                                            $fh -> push_read( chunk => 1, $main_accept_cb{
                                                                "${host}:${port}"
                                                            } ) if defined $fh;
                                                        },

                                                        f => sub {
            # warn "h: ${host}:${port} f";
                                                            return if $$ != $master_pid;

                                                            undef( @on_error );
                                                            undef( $fh );
                                                            undef( $guard );
                                                            undef( @stack );

                                                            $fh -> push_read( chunk => 1, $main_accept_cb{
                                                                "${host}:${port}"
                                                            } ) if defined $fh;
                                                        },
                                                    } );

                                                    $fh -> push_read( chunk => 1, $main_accept_cb{
                                                        "${host}:${port}"
                                                    } ) if defined $fh;
                                                }
                                            } );
                                        } );
                                    },
                                } );

                                $fh -> push_read( chunk => 1, $main_accept_cb{ "${host}:${port}" } ) if defined $fh;

                            }, sub { 3 } );
                        } );
                    }
                }

                if( defined( my $code = shift( @stack ) ) ) {

                    $code -> ();
                }

                EV::run EV::RUN_NOWAIT;
            }
        } );

        my %cvs = ();

        while( true ) {

            EV::run EV::RUN_NOWAIT;

            if( can_spawn_worker( 'processor' ) ) {

                foreach my $event ( sort( { rand() <=> rand() } keys( %events ) ) ) {

                    my $seqs = $db -> get_bulk( [ "winseq:${event}", "inseq:${event}" ] );

                    if( ref( $seqs ) eq 'HASH' ) {

                        my $current_win_seq = $seqs -> { "winseq:${event}" };
                        my $current_in_seq = ( exists $seqs -> { "inseq:${event}" }
                            ? counter_to_dec( $seqs -> { "inseq:${event}" } )
                            : 0
                        );

                        if( ( $current_win_seq // 0 ) < $current_in_seq ) {

                            my $data = $db -> get( "in:${event}:" . ( $current_win_seq // 0 ) );

                            my $rv = $db -> cas(
                                "winseq:${event}",
                                $current_win_seq,
                                ( ( $current_win_seq // 0 ) + 1 ),
                            );

                            unless( $rv ) {

                                die( 'cas(): ' . $db -> error() );
                            }

                            if( defined $data ) {

                                $wip{ ( $current_win_seq // 0 ) } = time();

                                my $pid = spawn_worker( processor => [ $event, $data ] );
                                my $cv = read_from( $pid, 1, sub {

                                    my ( $data ) = @_;

                                    if( $data eq 'k' ) {

                                        delete( $wip{ ( $current_win_seq // 0 ) } );

                                        my $rv = $db -> remove(
                                            "in:${event}:" . ( $current_win_seq // 0 )
                                        );

                                        unless( $rv ) {

                                            warn( 'remove(): ' . $db -> error() );
                                        }
                                    }
                                } );

                                $cvs{ $pid } = $cv;

                                $cv -> cb( sub {

                                    kill( SIGTERM, $pid );
                                    delete( $cvs{ $pid } );
                                } );

                                last;
                            }
                        }

                    } else {

                        die( 'get_bulk(): ' . $db -> error() );
                    }
                }
            }

            foreach my $event ( sort( { rand() <=> rand() } keys( %replication ) ) ) {

                foreach my $hostname ( sort( { rand() <=> rand() }
                    keys( %{ $replication{ $event } } ) ) ) {

                    foreach my $peer_port ( sort( { rand() <=> rand() }
                        keys( %{ $replication{ $event } -> { $hostname } } ) ) ) {

                        EV::run EV::RUN_NOWAIT;

                        my $suffix = "${event}:${hostname}:${peer_port}";
                        my $seqs = $db -> get_bulk( [
                            "wrinseq:${suffix}",
                            "rinseq:${suffix}",
                        ] );

                        if( ref( $seqs ) eq 'HASH' ) {

                            my $current_win_seq = $seqs -> { "wrinseq:${suffix}" };
                            my $current_in_seq = ( exists $seqs -> { "rinseq:${suffix}" }
                                ? counter_to_dec( $seqs -> { "rinseq:${suffix}" } )
                                : 0
                            );

                            if( ( $current_win_seq // 0 ) < $current_in_seq ) {

                                my $data = $db -> get_bulk( [
                                    "rin:${suffix}:" . ( $current_win_seq // 0 ),
                                    "rts:${suffix}:" . ( $current_win_seq // 0 ),
                                    "rid:${suffix}:" . ( $current_win_seq // 0 ),
                                    "rpr:${suffix}:" . ( $current_win_seq // 0 ),
                                ] );

                                if( ref( $data ) eq 'HASH' ) {

                                    my ( $data, $time, $id, $replicas ) = @$data{
                                        "rin:${suffix}:" . ( $current_win_seq // 0 ),
                                        "rts:${suffix}:" . ( $current_win_seq // 0 ),
                                        "rid:${suffix}:" . ( $current_win_seq // 0 ),
                                        "rpr:${suffix}:" . ( $current_win_seq // 0 ),
                                    };

                                    unless( defined $data && defined $time && defined $id ) {

                                        require Data::Dumper;

                                        warn(
                                            'Some data is missing: '
                                            . Data::Dumper::Dumper( [
                                                $data, $time, $id,
                                            ] )
                                        );
                                    }

                                    if( exists $failover{ $event } -> { $hostname }
                                        -> { $peer_port } -> { $id } ) {

                                        next;
                                    }

                                    if(
                                        exists $no_failover{ $event } -> { $hostname }
                                            -> { $peer_port } -> { $id }

                                        && ( ( $no_failover{ $event } -> { $hostname }
                                            -> { $peer_port } -> { $id } + NO_FAILOVER_TIME )
                                                > time() )
                                    ) {

                                        next;
                                    }

                                    if( ( $time + $repl_check_time ) <= time() ) {

                                        my $clean = sub {

                                            $db -> remove_bulk( [
                                                "rin:${suffix}:" . ( $current_win_seq // 0 ),
                                                "rts:${suffix}:" . ( $current_win_seq // 0 ),
                                                "rid:${suffix}:" . ( $current_win_seq // 0 ),
                                                "rre:${suffix}:" . $id,
                                                ( defined $replicas ? (
                                                    "rpr:${suffix}:" . ( $current_win_seq // 0 ),
                                                ) : () ),
                                            ] );
                                        };

                                        my $unfailover = sub {

                                            delete( $failover{ $event } -> { $hostname }
                                                -> { $peer_port } -> { $id } );

                                            delete( $no_failover{ $event } -> { $hostname }
                                                -> { $peer_port } -> { $id } );
                                        };

                                        my $save_event = sub {

                                            my @stack = ();
                                            my $replicas = $replicas;
                                            my $acceptances = 0;
                                            my @clean_stack = ();
                                            my $count_replicas = 0;

                                            while( length( $replicas // '' ) > 0 ) {

                                                my $length = substr( $replicas, 0, 4, '' );
                                                $length = unpack( 'N', $length );

                                                my $host = substr( $replicas, 0, $length, '' );
                                                my $port = substr( $replicas, 0, 4, '' );
                                                $port = unpack( 'N', $port );

                                                ++$count_replicas;

                                                if( exists $peers{ $hostname }
                                                    -> { $peer_port } ) {

                                                    push( @stack, sub {

                                                        my $ok = false;
                                                        my $peer = $peers{ $hostname }
                                                            -> { $peer_port };

                                                        $peer -> { 'on_error' } -> ( sub {

                                                            return if $ok;
                                                            return if $$ != $master_pid;

                                                            ++$acceptances;

                                                            if( defined( my $next = shift( @stack ) ) ) {

                                                                $next -> ();
                                                            }
                                                        } );

                                                        my $fh = $peer -> { 'fh' };

                                                        $fh -> push_write(
                                                            'i'
                                                            . pack( 'N', length( $hostname ) )
                                                            . $hostname, pack( 'N', $peer_port )
                                                            . pack( 'N', length( $event ) )
                                                            . $event . pack( 'Q', $id )
                                                        );

                                                        push( @{ $variable_accept{
                                                            $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                        } }, {

                                                            k => sub {

                                                                return if $$ != $master_pid;
                                                                $ok = true;

                                                                ++$acceptances;

                                                                push( @clean_stack, sub {

                                                                    $fh -> push_write(
                                                                        'x'
                                                                        . pack( 'N', length( $hostname ) )
                                                                        . $hostname, pack( 'N', $peer_port )
                                                                        . pack( 'N', length( $event ) )
                                                                        . $event . pack( 'Q', $id )
                                                                    );
                                                                } );

                                                                $fh -> push_read( chunk => 1, $main_accept_cb{
                                                                    $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                                } ) if defined $fh;

                                                                if( defined( my $next = shift( @stack ) ) ) {

                                                                    $next -> ();
                                                                }
                                                            },

                                                            f => sub {

                                                                return if $$ != $master_pid;
                                                                $ok = true;

                                                                unless( $clean -> () ) {

                                                                    warn( 'remove_bulk(): ' . $db -> error() );
                                                                }

                                                                $unfailover -> ();
                                                                undef( @clean_stack );
                                                                undef( @stack );

                                                                $fh -> push_read( chunk => 1, $main_accept_cb{
                                                                    $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                                } ) if defined $fh;
                                                            },
                                                        } );

                                                        $fh -> push_read( chunk => 1, $main_accept_cb{
                                                            $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                        } ) if defined $fh;
                                                    } );

                                                } else {

                                                    ++$acceptances;
                                                }
                                            }

                                            push( @stack, sub {

                                                undef( @stack );

                                                if( $acceptances == $count_replicas ) {

                                                    unless( exists $failover{ $event } -> { $hostname }
                                                        -> { $peer_port } -> { $id } ) {

                                                        undef( @clean_stack );
                                                        return;
                                                    }

                                                    if(
                                                        exists $no_failover{ $event } -> { $hostname }
                                                            -> { $peer_port } -> { $id }

                                                        && ( ( $no_failover{ $event } -> { $hostname }
                                                            -> { $peer_port } -> { $id } + NO_FAILOVER_TIME )
                                                                > time() )
                                                    ) {

                                                        undef( @clean_stack );
                                                        return;
                                                    }

                                                    my $rv = $db -> transaction( sub {

                                                        my $task_id = asd -> save_event(
                                                            db => $db,
                                                            data => $data,
                                                            node => $events{ $event } //= {},
                                                            event => $event,
                                                        );

                                                        $failover{ $event } -> { $hostname }
                                                            -> { $peer_port } -> { $id } = $task_id;

                                                        $clean -> ();

                                                        1;
                                                    } );

                                                    unless( $rv ) {

                                                        die( 'save_event(): ' . $db -> error() );
                                                    }

                                                    while( defined( my $code = shift( @clean_stack ) ) ) {

                                                        $code -> ();
                                                    }

                                                    undef( @clean_stack );
                                                }

                                                $unfailover -> ();
                                            } );

                                            shift( @stack ) -> ();
                                        };

                                        $unfailover -> ();

                                        $failover{ $event } -> { $hostname }
                                             -> { $peer_port } -> { $id } = '';

                                        if( exists $peers{ $hostname }
                                            -> { $peer_port } ) {

                                            my $ok = false;
                                            my $peer = $peers{ $hostname }
                                                -> { $peer_port };

                                            $peer -> { 'on_error' } -> ( sub {

                                                return if $ok;
                                                return if $$ != $master_pid;

                                                $save_event -> ();
                                            } );

                                            my $fh = $peer -> { 'fh' };

                                            $fh -> push_write(
                                                'c' . pack( 'N', length( $event ) )
                                                . $event . pack( 'Q', $id )
                                                . pack( 'N', length( $data ) )
                                                . $data
                                            );

                                            push( @{ $variable_accept{
                                                $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                            } }, {

                                                k => sub {

                                                    return if $$ != $master_pid;
                                                    $ok = true;

                                                    unless( $clean -> () ) {

                                                        warn( 'remove_bulk(): ' . $db -> error() );
                                                    }

                                                    $unfailover -> ();

                                                    $fh -> push_read( chunk => 1, $main_accept_cb{
                                                        $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                    } ) if defined $fh;
                                                },

                                                f => sub {

                                                    return if $$ != $master_pid;
                                                    $ok = true;

                                                    my $rv = $db -> transaction( sub {

                                                        asd -> repl_save(
                                                            db => $db,
                                                            id => $id,
                                                            data => $data,
                                                            node => $replication{ $event }
                                                                -> { $hostname }
                                                                -> { $peer_port },
                                                            event => $event,
                                                            hostname => $hostname,
                                                            replicas => $replicas,
                                                            peer_port => $peer_port,
                                                        );

                                                        $clean -> ();

                                                        1;
                                                    } );

                                                    unless( $rv ) {

                                                        die( 'repl_save(): ' . $db -> error() );
                                                    }

                                                    $unfailover -> ();

                                                    $fh -> push_read( chunk => 1, $main_accept_cb{
                                                        $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                                    } ) if defined $fh;
                                                },
                                            } );

                                            $fh -> push_read( chunk => 1, $main_accept_cb{
                                                $peer -> { 'host' } . ':' . $peer -> { 'conn_port' }
                                            } ) if defined $fh;

                                        } else {

                                            $save_event -> ();
                                        }

                                        $db -> cas(
                                            "wrinseq:${suffix}",
                                            $current_win_seq,
                                            ( ( $current_win_seq // 0 ) + 1 ),
                                        );

                                    } else {

                                        next;
                                    }

                                } else {

                                    die( 'get_bulk(): ' . $db -> error() );
                                }
                            }

                        } else {

                            die( 'get_bulk(): ' . $db -> error() );
                        }
                    }
                }
            }
        }
    },
};

daemon_main( 'main' );

exit 0;

sub counter_to_dec {

    my ( $value ) = @_;

    return hex( '0x' . join( '', map( { sprintf( '%.2x', ord( $_ ) ) } split( //, $value ) ) ) );
}

__END__
