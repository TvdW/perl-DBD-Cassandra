package DBD::Cassandra::dr;
use v5.14;
use warnings;

use DBD::Cassandra::Connection;

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

        $attr_name= "cass_$attr_name" unless $attr_name =~ /\A$driver_prefix/o;

        $attr->{$attr_name}= $attr_val;
    }

    my $keyspace= delete $attr->{cass_database} || delete $attr->{cass_db} || delete $attr->{cass_keyspace};
    my $host= delete $attr->{cass_host} || 'localhost';
    my $port= delete $attr->{cass_port} || 9042;
    my $global_consistency= delete $attr->{cass_consistency};

    my $connection;
    eval {
        $connection= DBD::Cassandra::Connection->connect($host, $port, $user, $auth, {
            map { exists $attr->{"cass_$_"} ? ($_ => $attr->{"cass_$_"}) : () }
                qw/compression cql_version read_timeout write_timeout connect_timeout/
        });
        1;
    } or do {
        my $err= $@ || "unknown error";
        return $drh->set_err($DBI::stderr, "Can't connect to $dr_dsn: $err");
    };

    my ($outer, $dbh)= DBI::_new_dbh($drh, { Name => $dr_dsn });

    $dbh->STORE('Active', 1);
    $dbh->{cass_connection}= $connection;
    $dbh->{cass_consistency}= $global_consistency;

    $outer->do("use $keyspace") or return
        if $keyspace;

    return $outer;
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
