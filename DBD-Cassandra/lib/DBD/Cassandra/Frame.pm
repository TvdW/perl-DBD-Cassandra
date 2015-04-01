package DBD::Cassandra::Frame;
use v5.14;
use warnings;

require Exporter;
our @ISA= 'Exporter';
our @EXPORT_OK= qw(
    send_frame2
    recv_frame2
);

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
