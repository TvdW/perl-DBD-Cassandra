package DBD::Cassandra::Connection;
use v5.14;
use warnings;

use IO::Socket::INET;
use IO::Socket::Timeout;
use Socket qw/TCP_NODELAY IPPROTO_TCP/;
use DBD::Cassandra::Protocol qw/:all/;

require Compress::Snappy; # Don't import compress() / decompress() into our scope please.
require Compress::LZ4; # Don't auto-import your subs.
use Authen::SASL;

sub connect {
    my ($class, $host, $port, $user, $auth, $compression, $cql_version, $timeout)= @_;
    my $socket= IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        ( $timeout ? ( Timeout => $timeout ) : () ),
    ) or die "Can't connect: $@";

    $socket->setsockopt(IPPROTO_TCP, TCP_NODELAY, 1);
    if ($timeout) {
        IO::Socket::Timeout->enable_timeouts_on($socket);
        $socket->read_timeout($timeout);
        $socket->write_timeout($timeout);
    }

    my $self= bless {
        socket => $socket,
        Active => 1,
    }, $class;

    {
        my ($opcode, $body)= $self->request(
            OPCODE_OPTIONS,
            '',
            NO_RETRY
        );

        my $supported= unpack_string_multimap($body);

        # Try to find a somewhat sane compression format to use as a default
        my %compression_supported= map { $_ => 1 } @{$supported->{COMPRESSION}};
        if (!$compression) {
            $compression= 'lz4' if $compression_supported{lz4};
            $compression= 'snappy' if $compression_supported{snappy};
        }
        $compression= '' if $compression && $compression eq 'none';

        if ($compression && !$compression_supported{$compression}) {
            die "Tried to select a compression format the server does not understand: $compression";
        }

        # Use the latest CQL version supported unless we specify a version
        my %cql_supported= map { $_ => 1 } @{$supported->{CQL_VERSION}};
        if (!$cql_version) {
            ($cql_version)= reverse sort keys %cql_supported;
        }

        if (!$cql_version) {
            die 'Did not pick a CQL version. Are we talking to a Cassandra server?';
        }
        if (!$cql_supported{$cql_version}) {
            die 'Tried to pick a CQL version the server does not understand.';
        }
    }

    $self->{compression}= $compression;

    {
        my ($opcode, $body)= $self->request(
            OPCODE_STARTUP,
            pack_string_map({
                CQL_VERSION => $cql_version,
                ($compression ? ( COMPRESSION => $compression ) : ()),
            }),
            NO_RETRY
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
    my ($self, $opcode, $body, $retry)= @_;

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
        if ($retry && $retry > 0 && $DBD::Cassandra::Protocol::retryable{$code}) {
            return $self->request($opcode, $body, $retry-1);
        }
        die "$code: $message";
    }

    return ($r_opcode, $r_body);
}

sub send_frame2 {
    my ($self, $flags, $streamID, $opcode, $body)= @_;
    my $fh= $self->{socket};
    return print $fh pack("CCCCN/a", 2, $flags, $streamID, $opcode, $body);
}

sub recv_frame2 {
    my ($self)= @_;
    my $fh= $self->{socket};

    (read($fh, my $header, 8) == 8) #XXX Do we need to handle the case where we get less than 8 bytes?
        or die $self->unrecoverable_error("Failed to read reply header from server: $!");

    my ($version, $flags, $streamID, $opcode, $bodylen)=
        unpack('CCCCN', $header);

    return if ($version & 0x7f) != 2;

    my $body;
    if ($bodylen) {
        read $fh, $body, $bodylen #XXX What if we read slightly less than that?
            or die $self->unrecoverable_error("Failed to read reply from server: $!");
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
        NO_RETRY
    );

    while ($opcode == OPCODE_AUTH_CHALLENGE && $client->need_step) {
        my $last_response= unpack_bytes($body);
        ($opcode, $body)= $self->request(
            OPCODE_AUTH_RESPONSE,
            pack_bytes($client->client_step($last_response)),
            NO_RETRY
        );
    }

    if ($opcode == OPCODE_AUTH_SUCCESS) {
        # Done!
    } else {
        die "Unexpected reply from server";
    }
}

1;
