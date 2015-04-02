package DBD::Cassandra::Connection;
use v5.14;
use warnings;

use IO::Socket::INET;
use DBD::Cassandra::Protocol qw/:all/;

sub connect {
    my ($class, $host, $port, $user, $auth)= @_;
    my $socket= IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
    ) or die "Can't connect: $@";

    my $self= bless {
        socket => $socket,
    }, $class;

    my ($opcode, $body)= $self->request(
        OPCODE_STARTUP,
        pack_string_map({
            CQL_VERSION => "3.0.0"
        })
    );

    if ($opcode != OPCODE_READY) {
        die "Server sent an unsupported opcode";
    }

    return bless {
        socket => $socket,
    }, $class;
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

sub request {
    my ($self, $opcode, $body)= @_;
    send_frame2($self->{socket}, 0, 1, $opcode, $body)
        or die "Unable to send frame with opcode $opcode: $!";

    my ($r_flags, $r_stream, $r_opcode, $r_body)= recv_frame2($self->{socket});
    if ($r_stream != 1) {
        die $self->unrecoverable_error("Received an unexpected reply from the server");
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
