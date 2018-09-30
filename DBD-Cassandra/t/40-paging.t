use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 101;

my $dbh= TestCassandra->get(undef, Warn => 1, PrintWarn => 0, PrintError => 0);
ok($dbh);

my $keyspace= "dbd_cassandra_tests";

$dbh->do("drop keyspace if exists $keyspace");
$dbh->do("create keyspace $keyspace with replication={'class': 'SimpleStrategy', 'replication_factor': 1}");
$dbh->do("create table $keyspace.test_int (id bigint primary key)");

for (1..50) {
    is($dbh->do("insert into $keyspace.test_int (id) values (?)", undef, $_), '0E0');
}

my %seen;
my $sth= $dbh->prepare("select * from $keyspace.test_int", { PerPage => 5 });
$sth->execute;
while (my $row= $sth->fetchrow_arrayref()) {
    $seen{$row->[0]}= 1;
}
for (1..50) {
    is($seen{$_}, 1);
}

$dbh->disconnect;
