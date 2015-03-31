package DBD::Cassandra::dr;
use v5.14;
use warnings;

use IO::Socket::INET;
use DBD::Cassandra::Protocol qw/:all/;

# "*FIX ME* Explain what the imp_data_size is, so that implementors aren't
#  practicing cargo-cult programming" - DBI::DBD docs
$DBD::Cassandra::dr::imp_data_size = 0;

sub connect {
    my ($drh, $dr_dsn, $user, $auth, $attr)= @_;

    # Iterate through the DSN, write to $attr
    my $driver_prefix= 'cass_';
    for my $var (split /;/, $dr_dsn) {
        my ($attr_name, $attr_val)= split '=', $var, 2;
        return $drh->set_err($DBI::stderr, "Can't parse DSN part '$var'")
            unless defined $attr_val;

        $attr_name= "cass_$attr_name" unless $attr_name =~ /^$driver_prefix/o;

        $attr->{$attr_name}= $attr_val;
    }

    my $keyspace= delete $attr->{cass_database} || delete $attr->{cass_db} || delete $attr->{cass_keyspace}
        or return $drh->set_err($DBI::stderr, "Could not determine keyspace from DSN '$dr_dsn'");
    my $host= delete $attr->{cass_host} || 'localhost';
    my $port= delete $attr->{cass_port} || 9042;

    my $connection;
    eval {
        $connection= cass_connect($host, $port, $keyspace, $user, $auth);
        1;
    } or do {
        my $err= $@ || "unknown error";
        return $drh->set_err($DBI::stderr, "Can't connect to $dr_dsn: $err");
    };

    my ($outer, $dbh)= DBI::_new_dbh($drh, { Name => $dr_dsn });

    $dbh->STORE('Active', 1);
    $dbh->{cass_connection}= $connection;

    $outer->do("use $keyspace");

    return $outer;
}

sub cass_connect {
    my ($host, $port, $keyspace, $user, $auth)= @_;
    my $socket= IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
    ) or die "Can't connect: $@";

    {
        my $body= pack_string_map({ CQL_VERSION => '3.0.0' });
        send_frame( $socket, 2, 0, 1, OPCODE_STARTUP, $body ) or die "Could not send STARTUP: $!";
    }

    {
        my ($version, $flags, $streamid, $opcode, $body)= recv_frame($socket);
        if ($version != (2 | 0x80)) {
            die "Unknown CQLSH version sent by server";
        }

        if ($streamid != 1) {
            die "Server replied with a wrong StreamID";
        }

        if ($opcode != OPCODE_READY) {
            die "Server sent an unsupported opcode";
        }
    }

    return $socket;
}

sub data_sources {
    my ($drh, $attr)= @_;
    my @array= (qw/dbi:Cassandra/);
    return @array;
}

sub disconnect_all {
    # TODO: not needed?
}

1;
