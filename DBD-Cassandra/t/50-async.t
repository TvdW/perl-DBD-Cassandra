use v5.14;
use DBI;
use Test::More;

unless ($ENV{CASSANDRA_HOST}) {
    plan skip_all => "CASSANDRA_HOST not set";
}

plan tests => 3;

my $dbh= DBI->connect("dbi:Cassandra:host=$ENV{CASSANDRA_HOST};keyspace=dbd_cassandra_tests;read_timeout=5;connect_timeout=5;write_timeout=5", undef, undef, {RaiseError => 1});
ok($dbh);

$dbh->do('create table if not exists test_async (id bigint primary key)');
$dbh->do('truncate test_async');

my @pending;
for my $i (1..100) {
    my $sth= $dbh->prepare("insert into test_async (id) values (?)", {async => 1});
    $sth->execute($i);
    push @pending, $sth;
}

my $ok= 1;
$ok &&= $_->x_finish_async for reverse @pending;
ok($ok);

my $rows= $dbh->selectall_arrayref('select * from test_async');
ok(0+@$rows == 100);

$dbh->disconnect;
