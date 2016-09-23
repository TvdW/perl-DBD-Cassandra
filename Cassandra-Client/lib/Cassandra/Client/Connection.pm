package Cassandra::Client::Connection;
use 5.008;
use strict;
use warnings;
use vars qw/$BUFFER/;

use Ref::Util qw/is_arrayref/;
use IO::Socket::INET;
use Errno;
use Socket qw/SOL_SOCKET IPPROTO_TCP SO_KEEPALIVE TCP_NODELAY/;
use Cassandra::Client::Util;
use Cassandra::Client::Protocol qw/
    :constants
    %consistency_lookup
    %batch_type_lookup
    pack_bytes
    pack_longstring
    pack_queryparameters
    pack_shortbytes
    pack_stringmap
    pack_stringlist
    unpack_inet
    unpack_int
    unpack_metadata
    unpack_shortbytes
    unpack_string
    unpack_stringmultimap
/;
use Cassandra::Client::Encoder qw/
    make_encoder
/;
use Cassandra::Client::Decoder qw/
    make_decoder
/;
use Cassandra::Client::Error;
use Cassandra::Client::ResultSet;
use Scalar::Util qw/weaken/;

use constant STREAM_ID_LIMIT => 32768;

# Populated at BEGIN{} time
my @compression_preference;
my %available_compression;

sub new {
    my ($class, %args)= @_;

    my $self= bless {
        client          => $args{client},
        async_io        => $args{async_io},
        pool_id         => undef,

        options         => $args{options},
        request_timeout => $args{options}{request_timeout},
        host            => $args{host},
        metadata        => $args{metadata},
        prepare_cache   => $args{metadata}->prepare_cache,
        last_stream_id  => 0,
        pending_streams => {},

        decompress_func => undef,
        compress_func   => undef,
        connected       => 0,
        connecting      => undef,
        socket          => undef,
        fileno          => undef,
        pending_write   => undef,
        shutdown        => 0,
        read_buffer     => \(my $empty= ''),
    }, $class;
    weaken($self->{async_io});
    weaken($self->{client});
    return $self;
}

sub get_local_status {
    my ($self, $callback)= @_;

    series([
        sub {
            my ($next)= @_;
            $self->execute_prepared($next, \"select key, data_center, host_id, broadcast_address, rack, release_version, tokens from system.local", undef, { consistency => 'local_one' });
        },
        sub {
            my ($next, $result)= @_;

            my %local_status= map { $_->[3] => {
                peer => $_->[3],
                data_center => $_->[1],
                host_id => $_->[2],
                preferred_ip => $_->[3],
                rack => $_->[4],
                release_version => $_->[5],
                tokens => $_->[6],
            } } @{$result->rows};

            $next->(undef, \%local_status);
        },
    ], $callback);

    return;
}

sub get_peers_status {
    my ($self, $callback)= @_;

    series([
        sub {
            my ($next)= @_;
            $self->execute_prepared($next, \"select peer, data_center, host_id, preferred_ip, rack, release_version, tokens from system.peers", undef, { consistency => 'local_one' });
        },
        sub {
            my ($next, $result)= @_;

            my %network_status= map { $_->[0] => {
                peer => $_->[0],
                data_center => $_->[1],
                host_id => $_->[2],
                preferred_ip => $_->[3],
                rack => $_->[4],
                release_version => $_->[5],
                tokens => $_->[6],
            } } @{$result->rows};

            $next->(undef, \%network_status);
        },
    ], $callback);

    return;
}

sub get_network_status {
    my ($self, $callback)= @_;

    parallel([
        sub {
            my ($next)= @_;
            $self->get_peers_status($next);
        },
        sub {
            my ($next)= @_;
            $self->get_local_status($next);
        },
    ], sub {
        my ($error, $peers, $local)= @_;
        if ($error) { return $callback->($error); }
        return $callback->(undef, { %$peers, %$local });
    });
}

sub register_events {
    my ($self, $callback)= @_;

    $self->request($callback, OPCODE_REGISTER, pack_stringlist([
        'TOPOLOGY_CHANGE',
        'STATUS_CHANGE',
    ]));

    return;
}


###### QUERY CODE
sub execute_prepared {
    my ($self, $callback, $queryref, $parameters, $attr)= @_;

    # Note: parameters is retained until the query is complete. It must not be changed; clone if needed.
    # Same for attr. Note that external callers automatically have their arguments cloned.

    my $prepared= $self->{prepare_cache}{$$queryref} or do {
        return $self->prepare_and_try_execute_again($callback, $queryref, $parameters, $attr);
    };

    my $want_result_metadata= !$prepared->{result_metadata}{columns};
    my $row;
    if ($parameters) {
        eval {
            $row= $prepared->{encoder}->($parameters);
            1;
        } or do {
            my $error= $@ || "??";
            return $callback->("Failed to encode row to native protocol: $error");
        };
    }

    my $chosen_consistency= $attr->{consistency} || $self->{options}{default_consistency};
    my $consistency= ($chosen_consistency ? $consistency_lookup{$chosen_consistency} : CONSISTENCY_ONE);
    if (!defined $consistency) {
        return $callback->("Invalid consistency level specified");
    }

    my $page_size= (0+($attr->{page_size} || $self->{options}{max_page_size} || 0)) || undef;
    my $paging_state= $attr->{page} || undef;
    my $execute_body= pack_shortbytes($prepared->{id}).pack_queryparameters($consistency, !$want_result_metadata, $page_size, $paging_state, undef, $row);

    my $on_completion= sub {
        # my ($body)= $_[2]; (not copying, because performance. assuming ownership)
        my ($err, $code)= @_;

        if ($err) {
            if (ref $err && $err->code == 0x2500) {
                return $self->prepare_and_try_execute_again($callback, $queryref, $parameters, $attr);
            }
            return $callback->($err);
        }

        if ($code != OPCODE_RESULT) {
            return $callback->("Expected a RESULT frame but got something else; considering the query failed");
        }

        $self->decode_result($callback, $prepared, $_[2]);
    };

    $self->request($on_completion, OPCODE_EXECUTE, $execute_body);

    return;
}

sub prepare_and_try_execute_again {
    my ($self, $callback, $queryref, $parameters, $attr)= @_;

    if ($attr->{_prepared_and_tried_again}++) {
        return $callback->("Query failed because it seems to be missing from the server's prepared statement cache");
    }

    series([
        sub {
            my ($next)= @_;
            $self->prepare($next, $$queryref);
        },
    ], sub {
        return $callback->($_[0]) if $_[0];

        unless ($self->{prepare_cache}{$$queryref}) {
            # We're recursing, so let's make sure we avoid the infinite loop
            return $callback->("Internal error: expected query to be prepared but it was not");
        }

        return $self->execute_prepared($callback, $queryref, $parameters, $attr);
    });
    return;
}

sub execute_batch {
    my ($self, $callback, $queries, $attribs)= @_;
    # Like execute_prepared, assumes ownership of $queries and $attribs

    if (!is_arrayref($queries)) {
        return $callback->("execute_batch: queries argument must be an array of arrays");
    }

    my @prepared;
    for my $query (@$queries) {
        if (!is_arrayref($query)) {
            return $callback->("execute_batch: entries in query argument must be arrayrefs");
        }
        if (!$query->[0]) {
            return $callback->("Empty or no query given, cannot execute as part of a batch");
        }
        if ($query->[1] && !is_arrayref($query->[1])) {
            return $callback->("Query parameters to batch() must be given as an arrayref");
        }

        if (my $prep= $self->{prepare_cache}{$query->[0]}) {
            push @prepared, [ $prep, $query->[1] ];

        } else {
            return $self->prepare_and_try_batch_again($callback, $queries, $attribs);
        }
    }

    my $batch_type= 0;
    if ($attribs->{batch_type}) {
        $batch_type= $batch_type_lookup{$attribs->{batch_type}};
        if (!defined $batch_type) {
            return $callback->("Unknown batch_type: <$attribs->{batch_type}>");
        }
    }

    my $chosen_consistency= $attribs->{consistency} || $self->{options}{default_consistency};
    my $consistency= ($chosen_consistency ? $consistency_lookup{$chosen_consistency} : CONSISTENCY_ONE);
    if (!defined $consistency) {
        return $callback->("Invalid consistency level specified");
    }

    my $batch_frame= pack('Cn', $batch_type, (0+@prepared));
    for my $prep (@prepared) {
        $batch_frame .= pack('C', 1).pack_shortbytes($prep->[0]{id}).$prep->[0]{encoder}->($prep->[1]);
    }
    $batch_frame .= pack('nC', $consistency, 0);

    my $on_completion= sub {
        # my ($body)= $_[2]; (not copying, because performance. assuming ownership)
        my ($err, $code)= @_;

        if ($err) {
            if (ref $err && $err->code == 0x2500) {
                return $self->prepare_and_try_batch_again($callback, $queries, $attribs);
            }
            return $callback->($err);
        }

        if ($code != OPCODE_RESULT) {
            return $callback->("Expected a RESULT frame but got something else; considering the query failed");
        }

        $self->decode_result($callback, undef, $_[2]);
    };

    $self->request($on_completion, OPCODE_BATCH, $batch_frame);

    return;
}

sub prepare_and_try_batch_again {
    my ($self, $callback, $queries, $attribs)= @_;

    if ($attribs->{_prepared_and_tried_again}++) {
        return $callback->("Batch failed because one or more queries seem to be missing from the server's prepared statement cache");
    }

    my %to_be_prepared;
    $to_be_prepared{$_->[0]}= 1 for @$queries;

    parallel([
        map { my $query= $_; sub {
            my ($next)= @_;
            $self->prepare($next, $query);
        } } keys %to_be_prepared
    ], sub {
        return $callback->($_[0]) if $_[0];

        return $self->execute_batch($callback, $queries, $attribs);
    });
    return;
}

sub prepare {
    my ($self, $callback, $query)= @_;

    # XXX Should we guard against the case of preparing the same statement in parallel?

    series([
        sub {
            my ($next)= @_;
            my $req= pack_longstring($query);
            $self->request($next, OPCODE_PREPARE, $req);
        },
        sub {
            my ($next, $code, $body)= @_;
            if ($code != OPCODE_RESULT) {
                return $next->("Got unexpected failure while trying to prepare");
            }

            my $result_type= unpack_int($body);
            if ($result_type != RESULT_PREPARED) {
                return $next->("Unexpected response from server while preparing");
            }

            my $id= unpack_shortbytes($body);
            my $metadata= eval { unpack_metadata($body) } or return $next->("Unable to unpack query metadata: $@");
            my $resultmetadata= eval { unpack_metadata($body) } or return $next->("Unable to unpack query metadata: $@");

            my ($encoder, $decoder);
            eval {
                $encoder= make_encoder($metadata);
                $decoder= make_decoder($resultmetadata);
                1;
            } or do {
                my $error= $@ || "??";
                return $callback->("Error while preparing query, couldn't compile encoder/decoder: $error");
            };

            $self->{metadata}->add_prepared($query, $id, $metadata, $resultmetadata, $decoder, $encoder);
            $next->();
        },
    ], $callback);

    return;
}

sub decode_result {
    my ($self, $callback, $prepared)= @_; # $_[3]=$body

    my $result_type= unpack('l>', substr($_[3], 0, 4, ''));
    if ($result_type == RESULT_ROWS) { # Rows
        my $metadata= eval { unpack_metadata($_[3]) } or return $callback->("Unable to unpack query metadata: $@");
        my $rows;
        eval {
            my $decoder= $prepared->{decoder} || make_decoder($metadata);
            $rows= $decoder->($_[3]);
            1;
        } or do {
            my $error= $@ || "??";
            return $callback->("Error while decoding row: $error");
        };
        $callback->(undef,
            Cassandra::Client::ResultSet->new(
                $rows,
                [ map { $_->[2] } @{$prepared->{result_metadata}{columns} || $metadata->{columns}} ],
                $metadata->{paging_state},
            )
        );

    } elsif ($result_type == RESULT_VOID) { # Void
        return $callback->();

    } elsif ($result_type == RESULT_SET_KEYSPACE) { # Set_keyspace
        my $new_keyspace= unpack_string($_[3]);
        return $callback->();

    } elsif ($result_type == RESULT_SCHEMA_CHANGE) { # Schema change
        return $self->wait_for_schema_agreement($callback);

    } else {
        return $callback->("Query executed successfully but got an unexpected response type");
    }
    return;
}

sub wait_for_schema_agreement {
    my ($self, $callback)= @_;
    sleep 1; # I am very sorry.
    return $callback->();
}



###### PROTOCOL CODE
sub handshake {
    my ($self, $callback)= @_;
    series([
        sub { # Send the OPCODE_OPTIONS
            my ($next)= @_;
            $self->request($next, OPCODE_OPTIONS, '');
        },
        sub { # The server hopefully just told us what it supports, let's respond with a STARTUP message
            my ($next, $response_code, $body)= @_;
            if ($response_code != OPCODE_SUPPORTED) {
                return $next->("Server returned an unexpected handshake");
            }

            my $map= unpack_stringmultimap($body);

            unless ($map->{CQL_VERSION} && $map->{COMPRESSION}) {
                return $next->("Server did not return compression and cql version information");
            }

            my $selected_cql_version= $self->{options}{cql_version};
            if (!$selected_cql_version) {
                ($selected_cql_version)= reverse sort @{$map->{CQL_VERSION}};
            }

            my %ss_compression= map { $_, 1 } @{$map->{COMPRESSION}};
            my $selected_compression= $self->{options}{compression};
            if (!$selected_compression) {
                for (@compression_preference) {
                    if ($ss_compression{$_} && $available_compression{$_}) {
                        $selected_compression= $_;
                        last;
                    }
                }
            }
            $selected_compression= undef if $selected_compression && $selected_compression eq 'none';

            if ($selected_compression) {
                if (!$ss_compression{$selected_compression}) {
                    return $next->("Server did not support requested compression method <$selected_compression>");
                }
                if (!$available_compression{$selected_compression}) {
                    return $next->("Requested compression method <$selected_compression> is supported by the server but not by us");
                }
            }

            my $request_body= pack_stringmap({
                CQL_VERSION => $selected_cql_version,
                ($selected_compression ? (COMPRESSION => $selected_compression) : ()),
            });

            $self->request($next, OPCODE_STARTUP, $request_body);

            # This needs to happen after we send the STARTUP message
            $self->setup_compression($selected_compression);
        },
        sub { # By now we should know whether we need to authenticate
            my ($next, $response_code, $body)= @_;
            if ($response_code == OPCODE_READY) {
                return $next->(undef, $body); # Pass it along
            }

            if ($response_code == OPCODE_AUTHENTICATE) {
                return $self->authenticate($next, unpack_string($body));
            }

            return $next->("Unexpected response from the server");
        },
        sub {
            my ($next)= @_;
            if ($self->{options}{keyspace}) {
                return $self->execute_prepared($next, \('use "'.$self->{options}{keyspace}.'"'), undef, { consistency => 'local_one' });
            }
            return $next->();
        },
        sub {
            my ($next)= @_;
            if (!$self->{ipaddress}) {
                return $self->get_local_status($next);
            }
            return $next->();
        },
        sub {
            my ($next, $status)= @_;
            if ($status) {
                my ($local)= values %$status;
                $self->{ipaddress}= $local->{peer};
                $self->{datacenter}= $local->{data_center};
            }
            if (!$self->{ipaddress}) {
                return $next->("Unable to determine node's IP address");
            }
            return $next->();
        }
    ], $callback);

    return;
}

sub authenticate {
    my ($self, $callback, $authenticator)= @_;

    my $user= $self->{options}{username};
    my $pass= $self->{options}{password};

    if (!$user || !$pass) {
        return $callback->("Server expected authentication using <$authenticator> but no credentials were set");
    }

    series([
        sub {
            my ($next)= @_;
            my $auth_body= pack_bytes("\0$user\0$pass");
            $self->request($next, OPCODE_AUTH_RESPONSE, $auth_body);
        },
        sub {
            my ($next, $code, $body)= @_;
            if ($code == OPCODE_AUTH_SUCCESS) {
                $next->();
            } else {
                $next->("Failed to authenticate: unknown error");
            }
        },
    ], $callback);

    return;
}

sub handle_event {
    my ($self, $eventdata)= @_;
    my $type= unpack_string($eventdata);
    if ($type eq 'TOPOLOGY_CHANGE') {
        my ($change, $ipaddress)= (unpack_string($eventdata), unpack_inet($eventdata));
        $self->{client}->_handle_topology_change($change, $ipaddress);

    } elsif ($type eq 'STATUS_CHANGE') {
        my ($change, $ipaddress)= (unpack_string($eventdata), unpack_inet($eventdata));
        $self->{client}->_handle_status_change($change, $ipaddress);

    } else {
        warn 'Received unknown event type: '.$type;
    }
}

sub get_pool_id {
    $_[0]{pool_id}
}

sub set_pool_id {
    $_[0]{pool_id}= $_[1];
}

sub ip_address {
    $_[0]{ipaddress}
}



####### IO LOGIC
sub connect {
    my ($self, $callback)= @_;
    return $callback->() if $self->{connected};

    if ($self->{connecting}) {
        push @{$self->{connecting}}, $callback;
        return;
    }

    $self->{connecting}= [$callback];

    my $socket; {
        local $@;

        $socket= IO::Socket::INET->new(
            PeerAddr => $self->{host},
            PeerPort => $self->{options}{port},
            Proto    => 'tcp',
            Blocking => 0,
        );

        unless ($socket) {
            return $callback->("Could not connect: $@");
        }

        $socket->setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1);
        $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1);
    }

    $self->{socket}= $socket;
    $self->{fileno}= $socket->fileno;
    $self->setup_io;
    $self->handshake(sub {
        my $st= shift;
        $self->{connected}= 1;
        $_->($st) for @{$self->{connecting}};
        undef $self->{connecting};
    });

    return;
}

sub setup_io {
    my ($self)= @_;
    $self->{async_io}->register($self->{fileno}, $self);
    $self->{async_io}->register_read($self->{fileno});
    return;
}

sub request {
    # my $body= $_[3] (let's avoid copying that blob). Yes, this code assumes ownership of the body.
    my ($self, $cb, $opcode)= @_;
    return $cb->("Connection shutting down") if $self->{shutdown};

    my $pending= $self->{pending_streams};

    my $stream_id= $self->{last_stream_id} + 1;
    my $attempts= 0;
    while (exists($pending->{$stream_id}) || $stream_id >= STREAM_ID_LIMIT) {
        $stream_id= (++$stream_id) % STREAM_ID_LIMIT;
        return $cb->("Cannot find a stream ID to post query with") if ++$attempts >= STREAM_ID_LIMIT;
    }
    $self->{last_stream_id}= $stream_id;
    $pending->{$stream_id}= [$cb, $self->{async_io}->deadline($self->{fileno}, $stream_id, $self->{request_timeout})];

    WRITE: {
        my $flags= 0;

        if (length($_[3]) > 500 && (my $compress_func= $self->{compress_func})) {
            $flags |= 1;
            $compress_func->($_[3]);
        }

        my $data= pack('CCsCN/a', 3, $flags, $stream_id, $opcode, $_[3]);

        if (defined $self->{pending_write}) {
            # XXX Now that we have timeouts, we should consider not sending anything if we already timed out
            $self->{pending_write} .= $data;
            last WRITE;
        }

        my $length= length $data;
        my $result= syswrite($self->{socket}, $data, $length);
        if ($result && $result == $length) {
            # All good
        } elsif (defined $result || $!{EAGAIN}) {
            substr($data, 0, $result, '') if $result;
            $self->{pending_write}= $data;
            $self->{async_io}->register_write($self->{fileno});
        } else {
            my $error= $!;
            $self->shutdown(undef, $error);
        }
    }

    return;
}

sub can_read {
    my ($self)= @_;
    my $shutdown_when_done;
    local *BUFFER= $self->{read_buffer};
    my $bufsize= length $BUFFER;

READ:
    while (!$self->{shutdown}) {
        my $read_cnt= sysread($self->{socket}, $BUFFER, 10240, $bufsize);
        my $read_something;
        if ($read_cnt) {
            $bufsize += $read_cnt;
            $read_something= 1;

        } elsif (!defined $read_cnt) {
            if (!$!{EAGAIN}) {
                my $error= "$!";
                $shutdown_when_done= $error;
            }
        } elsif ($read_cnt == 0) { # EOF
            $shutdown_when_done= "Disconnected from server";
        }

READ_NEXT:
        goto READ_MORE if $bufsize < 9;
        my ($version, $flags, $stream_id, $opcode, $bodylen)= unpack('CCsCN', substr($BUFFER, 0, 9));
        if ($bufsize < $bodylen+9) {
            goto READ_MORE;
        }

        substr($BUFFER, 0, 9, '');
        my $body= substr($BUFFER, 0, $bodylen, '');
        $bufsize -= 9 + $bodylen;

        # Decompress if needed
        if (($flags & 1) && $body) {
            $self->{decompress_func}->($body);
        }

        if ($stream_id != -1) {
            my $stream_cb= delete $self->{pending_streams}{$stream_id};
            if (!$stream_cb) {
                if (!$self->{shutdown}) {
                    warn 'BUG: received response for unknown stream';
                } # Else: totally fine

            } elsif ($opcode == OPCODE_ERROR) {
                my ($code, $message)= unpack('Nn/a', $body);
                my ($cb, $dl)= @$stream_cb;
                $$dl= 1;
                $cb->(Cassandra::Client::Error->new($code, $message));

            } else {
                my ($cb, $dl)= @$stream_cb;
                $$dl= 1;
                $cb->(undef, $opcode, $body);
            }

        } else {
            $self->handle_event($body);
        }

        goto READ_NEXT;

READ_MORE:
        last READ unless $read_something;
    }

    if ($shutdown_when_done) {
        $self->shutdown(undef, $shutdown_when_done);
    }

    return;
}

sub can_write {
    my ($self)= @_;

    my $result= syswrite($self->{socket}, $self->{pending_write});
    if (!defined($result)) {
        if ($!{EAGAIN}) {
            return; # Huh. Oh well, whatever
        }

        my $error= "$!";
        undef $self->{pending_write}; #XXX remind me, why do we do that?
        return $self->shutdown(undef, $error);
    }
    if ($result == 0) { return; } # No idea whether that happens, but guard anyway.
    substr($self->{pending_write}, 0, $result, '');

    if (!length $self->{pending_write}) {
        $self->{async_io}->unregister_write($self->{fileno});
        delete $self->{pending_write};
    }

    return;
}

sub can_timeout {
    my ($self, $id)= @_;
    my $stream= delete $self->{pending_streams}{$id};
    $self->{pending_streams}{$id}= [ sub{}, \(my $zero= 0) ]; # fake it
    $stream->[0]->("Request timed out");
    return;
}

sub shutdown {
    my ($self, $cb, $shutdown_reason)= @_;

    if ($self->{shutdown}) {
        $cb->() if $cb;
        return;
    }

    $self->{shutdown}= 1;

    my $pending= $self->{pending_streams};
    $self->{pending_streams}= {};
    $_->[0]->("Disconnected: $shutdown_reason") for values %$pending;

    $self->{socket}->close;
    $self->{async_io}->unregister_read($self->{fileno});
    if (defined(delete $self->{pending_write})) {
        $self->{async_io}->unregister_write($self->{fileno});
    }
    $self->{async_io}->unregister($self->{fileno}, $self);
    $self->{client}->_disconnected($self->get_pool_id);

    $cb->() if $cb;

    return;
}



###### COMPRESSION
BEGIN {
    @compression_preference= qw/snappy lz4/;

    %available_compression= (
        snappy  => scalar eval "use Compress::Snappy (); 1;",
        lz4     => scalar eval "use Compress::LZ4 (); 1;",
    );
}

sub setup_compression {
    my ($self, $type)= @_;

    return unless $type;
    if ($type eq 'snappy') {
        $self->{compress_func}= \&compress_snappy;
        $self->{decompress_func}= \&decompress_snappy;
    } elsif ($type eq 'lz4') {
        $self->{compress_func}= \&compress_lz4;
        $self->{decompress_func}= \&decompress_lz4;
    } else {
        warn 'Internal error: failed to set compression';
    }

    return;
}

sub compress_snappy {
    $_[0]= Compress::Snappy::compress(\$_[0]);
    return;
}

sub decompress_snappy {
    if ($_[0] ne "\0") {
        $_[0]= Compress::Snappy::decompress(\$_[0]);
    } else {
        $_[0]= '';
    }
    return;
}

sub compress_lz4 {
    $_[0]= pack('N', length($_[0])) . Compress::LZ4::lz4_compress(\$_[0]);
    return;
}

sub decompress_lz4 {
    my $len= unpack('N', substr $_[0], 0, 4, '');
    if ($len) {
        $_[0]= Compress::LZ4::lz4_decompress(\$_[0], $len);
    } else {
        $_[0]= '';
    }
    return;
}

1;
