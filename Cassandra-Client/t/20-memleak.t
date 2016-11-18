#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Cassandra::Client;
use Cassandra::Client::Util qw/series parallel/;
use Scalar::Util 'weaken';

plan skip_all => "CASSANDRA_HOST not set" unless $ENV{CASSANDRA_HOST};
plan tests => 13;

{
    # Weaken() sanity
    my $h= {};
    weaken($h);
    ok(!$h) or diag("Our weaken() sucks.");
}

my $deinit;
BEGIN {
    no warnings;
    no strict 'refs';
    my $destroy= *{"Cassandra::Client::DESTROY"}{CODE};
    *Cassandra::Client::DESTROY= sub {
        $deinit= 1;
        goto &$destroy;
    };
}

my $client= Cassandra::Client->new( contact_points => [split /,/, $ENV{CASSANDRA_HOST}], username => $ENV{CASSANDRA_USER}, password => $ENV{CASSANDRA_AUTH}, anyevent => (rand()<.5) );
$client->connect();

my $db= 'perl_cassandra_client_tests';
$client->execute("create keyspace if not exists $db with replication={'class':'SimpleStrategy', 'replication_factor': 1}");
$client->execute("create table if not exists $db.test_int (id int primary key, value int)");
$client->execute("insert into $db.test_int (id, value) values (5, 6)");
{
    my ($result)= $client->execute("select id, value from $db.test_int where id=5");
    my $rows= $result->rows;
    ok(@$rows == 1);
    ok($rows->[0][0] == 5);
    ok($rows->[0][1] == 6);
}

$client->execute("delete from $db.test_int where id=5");
{
    my ($result)= $client->execute("select id, value from $db.test_int where id=5");
    my $rows= $result->rows;
    ok(@$rows == 0);
}

my @conns= values %{$client->{pool}{pool}};
weaken $_ for @conns;
ok(0+(grep $_, @conns));

ok(!$deinit);
$client->shutdown if rand() < 0.5;
weaken $client;
ok($deinit);

ok(!grep $_, @conns);

if (!$deinit) {
    if (eval("use Devel::Cycle; use Data::Dumper; 1")) {
        my $trivial_cycles;
        find_cycle($client, sub {
            $trivial_cycles= 1;
        });

        if ($trivial_cycles) {
            diag("Trivial cycles found, should be easy to fix.");
        } else {
            diag("No trivial cycles found, but we do have a memory leak!");
        }
    } else {
        diag("Skipping cycle check: can't load Devel::Cycle");
    }
}

# Test series()
{
    my $one= {};
    my $two= {};
    series([
        sub {
            shift->($one);
        },
        sub {
            shift->($one);
        },
    ], sub {
        $two->{abc}= 1;
    });

    weaken $one;
    weaken $two;
    ok(!$one);
    ok(!$two);
}

# Test parallel()
{
    my $one= {};
    my $two= {};
    parallel([
        sub { shift->($one); },
        sub { shift->($one); },
    ], sub {
        $two->{abc}= 1;
    });

    weaken $one;
    weaken $two;
    ok(!$one);
    ok(!$two);
}

1;
