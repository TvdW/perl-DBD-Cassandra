use 5.010;
use warnings;
use strict;
use File::Basename qw//; use lib File::Basename::dirname(__FILE__).'/lib';
use TestCassandra;
use Test::More;

plan skip_all => "Missing Cassandra test environment" unless TestCassandra->is_ok;
plan tests => 3;

my $dbh= TestCassandra->get(";keyspace=dbd_cassandra_tests");
ok($dbh);

$dbh->do('create table if not exists test_async (id bigint primary key)');
$dbh->do('truncate test_async');

my $count= 100000;
my (@pending, @reusable);
for my $i (1..$count) {
    my $sth= (shift @reusable) || $dbh->prepare("insert into test_async (id) values (?)", {async => 1});
    $sth->execute($i);
    push @pending, $sth;

    if (@pending > 5000) {
        my $pending_sth= shift @pending;
        $pending_sth->x_finish_async;
        push @reusable, $pending_sth;
    }
}

my $ok= 1;
$ok &&= $_->x_finish_async for reverse @pending;
ok($ok);

my $rows= $dbh->selectall_arrayref('select * from test_async');
ok(0+@$rows == $count) or diag("Expected $count rows, got ".(0+@$rows));

$dbh->disconnect;
