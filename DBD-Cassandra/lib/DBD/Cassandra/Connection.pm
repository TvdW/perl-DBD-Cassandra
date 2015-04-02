package DBD::Cassandra::Connection;
use v5.14;
use warnings;

use IO::Socket::INET;
use DBD::Cassandra::Protocol qw/:all/;

require Compress::Snappy; # Don't import compress() / decompress() into our scope please.
require Compress::LZ4; # Don't auto-import your subs.

sub connect {
    my ($class, $host, $port, $user, $auth, $compression, $cql_version)= @_;
    my $socket= IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
    ) or die "Can't connect: $@";

    my $self= bless {
        socket => $socket,
        compression => $compression,
    }, $class;

    my ($opcode, $body)= $self->request(
        OPCODE_STARTUP,
        pack_string_map({
            CQL_VERSION => $cql_version,
            COMPRESSION => $compression,
        })
    );

    if ($opcode != OPCODE_READY) {
        die "Server sent an unsupported opcode";
    }

    return $self;
}

sub close {
    my ($self)= @_;
    $self->{socket}->close;
    $self->{socket}= undef;
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

    send_frame2($self->{socket}, $flags, 1, $opcode, $body)
        or die "Unable to send frame with opcode $opcode: $!";

    my ($r_flags, $r_stream, $r_opcode, $r_body)= recv_frame2($self->{socket});
    if ($r_stream != 1) {
        die $self->unrecoverable_error("Received an unexpected reply from the server");
    }

    if (($r_flags & 1) && $r_body) {
        $self->decompress($r_body);
    }

    return ($r_opcode, $r_body);
}

sub send_frame2 {
    my ($fh, $flags, $streamID, $opcode, $body)= @_;
    return print $fh pack("CCCCN/a", 2, $flags, $streamID, $opcode, $body);
}

sub recv_frame2 {
    my ($fh)= @_;

    (read($fh, my $header, 8) == 8) or return; #XXX Do we need to handle this case?

    my ($version, $flags, $streamID, $opcode, $bodylen)=
        unpack('CCCCN', $header);

    return if ($version & 0x7f) != 2;

    my $body;
    if ($bodylen) {
        read $fh, $body, $bodylen or return; #XXX What if we read slightly less than that?
    }

    return ($flags, $streamID, $opcode, $body);
}

1;
