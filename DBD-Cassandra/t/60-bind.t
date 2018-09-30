use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 2;

my $dbh= TestCassandra->get(";consistency=quorum");
ok($dbh);

my $keyspace= "dbd_cassandra_tests";

$dbh->do("drop keyspace if exists $keyspace");
$dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
$dbh->do("create table $keyspace.test_int (id bigint primary key, val text, id2 uuid)");
my $sth1= $dbh->prepare("insert into $keyspace.test_int (id, val, id2) values (?, ?, ?)");
$sth1->bind_param(1, 1);
$sth1->bind_param(2, "test");
$sth1->bind_param(3, "12345678-1234-1234-1234-123412341234");
$sth1->execute;

my $sth2= $dbh->prepare("select id, val, id2 from $keyspace.test_int where id=?", {async=>1});
$sth2->bind_param(1, 1);
$sth2->execute;
my $row= $sth2->fetchall_arrayref()->[0];
is_deeply($row, [
    1,
    "test",
    "12345678-1234-1234-1234-123412341234",
]);

$dbh->disconnect;
