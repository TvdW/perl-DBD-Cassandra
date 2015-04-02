use v5.14;
use DBI;
use Test::More;

unless ($ENV{CASSANDRA_HOST}) {
    plan skip_all => "CASSANDRA_HOST not set";
}

plan tests => 2;

my $keyspace= "dbd_cassandra_tests";

for my $compression (qw/lz4 snappy/) {
    my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST};compression=$compression", undef, undef, {RaiseError => 1});
    ok($dbh);

    $dbh->do("drop keyspace if exists $keyspace");
    $dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");

    $dbh->do("create table $keyspace.test (id bigint primary key, b blob)");

    my $sth= $dbh->prepare("insert into $keyspace.test (id, b) values (?, ?)");
    $sth->execute(1, ('0' x 1000000));

    $dbh->disconnect;
}
