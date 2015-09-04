package DBD::Cassandra::Connection;
use v5.14;
use warnings;

use IO::Socket::INET;
use IO::Socket::Timeout;
use Socket qw/TCP_NODELAY IPPROTO_TCP SOL_SOCKET SO_KEEPALIVE/;
use DBD::Cassandra::Protocol qw/:all/;

use Compress::Snappy qw();
use Compress::LZ4 qw();
use Authen::SASL qw();

sub connect {
    my ($class, $host, $port, $user, $auth, $args)= @_;
    my $socket= IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        ($args->{connect_timeout} ? ( Timeout => $args->{connect_timeout} ) : () ),
    ) or die "Can't connect: $@";

    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1);
    $socket->setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1);
    if ($args->{read_timeout} || $args->{write_timeout}) {
        IO::Socket::Timeout->enable_timeouts_on($socket);
        $socket->read_timeout($args->{read_timeout}) if $args->{read_timeout};
        $socket->write_timeout($args->{write_timeout}) if $args->{write_timeout};
    }

    my $self= bless {
        socket => $socket,
        Active => 1,
    }, $class;

    {
        my ($opcode, $body)= $self->request(
            OPCODE_OPTIONS,
            '',
        );

        my $supported= unpack_string_multimap($body);

        # Try to find a somewhat sane compression format to use as a default
        my $compression= $args->{compression};
        my %compression_supported= map { $_ => 1 } @{$supported->{COMPRESSION}};
        if (!$compression) {
            $compression= 'lz4' if $compression_supported{lz4};
            $compression= 'snappy' if $compression_supported{snappy};
        }
        $compression= '' if $compression && $compression eq 'none';

        if ($compression && !$compression_supported{$compression}) {
            die "Tried to select a compression format the server does not understand: $compression";
        }
        $self->{compression}= $compression;

        # Use the latest CQL version supported unless we specify a version
        my $cql_version= $args->{cql_version};
        my %cql_supported= map { $_ => 1 } @{$supported->{CQL_VERSION}};
        if (!$cql_version) {
            ($cql_version)= reverse sort keys %cql_supported;
        }
        $self->{cql_version}= $cql_version;

        if (!$cql_version) {
            die 'Did not pick a CQL version. Are we talking to a Cassandra server?';
        }
        if (!$cql_supported{$cql_version}) {
            die 'Tried to pick a CQL version the server does not understand.';
        }
    }

    {
        my ($opcode, $body)= $self->request(
            OPCODE_STARTUP,
            pack_string_map({
                CQL_VERSION => $self->{cql_version},
                ($self->{compression} ? ( COMPRESSION => $self->{compression} ) : ()),
            }),
        );

        if ($opcode == OPCODE_AUTHENTICATE) {
            $self->authenticate($body, $user, $auth);

        } elsif ($opcode == OPCODE_READY) {
            # all good, nothing to do

        } else {
            die "Server sent an unsupported opcode";
        }
    }

    return $self;
}

sub close {
    my ($self)= @_;
    $self->{socket}->close if $self->{socket};
    $self->{socket}= undef;
    $self->{Active}= 0;
    1;
}

sub unrecoverable_error {
    my ($self, $message)= @_;
    $self->close;

    die "Unrecoverable: $message";
}

sub compress {
    my $self= shift;
    die "Requested to compress data but no compression format set"
        unless $self->{compression};

    if ($self->{compression} eq "snappy") {
        $_[0]= Compress::Snappy::compress(\$_[0]);
    } elsif ($self->{compression} eq "lz4") {
        $_[0]= pack('N', length($_[0])) . Compress::LZ4::lz4_compress(\$_[0]);
    }
}

sub decompress {
    my $self= shift;
    die "Requested to decompress data but no compression format set"
        unless $self->{compression};

    if ($self->{compression} eq "snappy") {
        # Compress::Snappy doesn't like returning an empty string.
        return "" if $_[0] eq "\0";

        $_[0]= Compress::Snappy::decompress(\$_[0])
            // die "Unable to decompress Snappy data";

    } elsif ($self->{compression} eq "lz4") {
        # Compress::LZ4 has a different interpretation of the LZ4 spec than Cassandra does
        my ($len)= unpack('N', substr $_[0], 0, 4, '');
        return if $len == 0;

        $_[0]= Compress::LZ4::lz4_decompress(\$_[0], $len)
            // die "Unable to decompress LZ4 data";

    } else {
        die "Unknown compression format";
    }
}

sub request {
    my ($self, $opcode, $body)= @_;

    my $flags= 0;
    if ($body && length($body) > 512 && $opcode != OPCODE_STARTUP && $self->{compression}) {
        $self->compress($body);
        $flags |= 1;
    }

    $self->send_frame2($flags, 1, $opcode, $body)
        or die $self->unrecoverable_error("Unable to send frame with opcode $opcode: $!");

    my ($r_flags, $r_stream, $r_opcode, $r_body)= $self->recv_frame2();
    if (!defined $r_flags) {
        die $self->unrecoverable_error("Server connection went away");
    }

    if ($r_stream != 1) {
        die $self->unrecoverable_error("Received an unexpected reply from the server");
    }

    if (($r_flags & 1) && $r_body) {
        $self->decompress($r_body);
    }

    if ($r_opcode == OPCODE_ERROR) {
        my ($code, $message)= unpack('Nn/a', $r_body);
        die "$code: $message";
    }

    return ($r_opcode, $r_body);
}

sub send_frame2 {
    my ($self, $flags, $streamID, $opcode, $body)= @_;
    my $fh= $self->{socket};
    return $fh->write(pack("CCCCN/a", 3, $flags, $streamID, $opcode, $body));
}

sub recv_frame2 {
    my ($self)= @_;
    my $fh= $self->{socket};

    my $read_bytes= read($fh, my $header, 8);
    if (!$read_bytes || $read_bytes != 8) {
        die $self->unrecoverable_error("Failed to read reply header from server: $!");
    }

    my ($version, $flags, $streamID, $opcode, $bodylen)=
        unpack('CCCCN', $header);

    return if ($version & 0x7f) != 2;

    my $body;
    if ($bodylen) {
        my $remaining= $bodylen;
        do {
            my $bytes= read $fh, my $chunk, $remaining
                or die $self->unrecoverable_error("Failed to read reply from server: $!");
            $body .= $chunk;
            $remaining -= $bytes;
        } while $remaining;
    }

    return ($flags, $streamID, $opcode, $body);
}

sub authenticate {
    my ($self, $authenticate_body, $user, $auth)= @_;

    die "Server requires authentication but we have no credentials defined"
        unless $user && $auth;

    #my $cls= unpack_string($authenticate_body);
    my $sasl= Authen::SASL->new(
        mechanism => 'PLAIN', # Cassandra doesn't seem to like it if we specify a space-separated list
        callback => {
            pass => sub { $auth },
            user => sub { $user },
        },
    );
    my $client= $sasl->client_new();

    my ($opcode, $body)= $self->request(
        OPCODE_AUTH_RESPONSE,
        pack_bytes($client->client_start()),
    );

    while ($opcode == OPCODE_AUTH_CHALLENGE && $client->need_step) {
        my $last_response= unpack_bytes($body);
        ($opcode, $body)= $self->request(
            OPCODE_AUTH_RESPONSE,
            pack_bytes($client->client_step($last_response)),
        );
    }

    if ($opcode == OPCODE_AUTH_SUCCESS) {
        # Done!
    } else {
        die "Unexpected reply from server";
    }
}

1;
