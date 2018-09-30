use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 6;

my $keyspace= "dbd_cassandra_tests";

for my $compression (qw/lz4 snappy none/) {
    my $dbh= TestCassandra->get(";compression=$compression");
    ok($dbh);

    $dbh->do("drop keyspace if exists $keyspace");
    $dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");

    $dbh->do("create table $keyspace.test (id bigint primary key, b blob)");

    my $original= '0' x 1000000;

    my $sth= $dbh->prepare("insert into $keyspace.test (id, b) values (?, ?)");
    $sth->execute(1, $original);

    my $row= $dbh->selectrow_arrayref("select b from $keyspace.test where id=1");
    ok($row->[0] eq $original);

    $dbh->disconnect;
}
