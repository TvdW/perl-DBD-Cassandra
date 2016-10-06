#!perl
use 5.010;
use strict;
use warnings;
use Test::More;
use Cassandra::Client;
use AnyEvent;

plan skip_all => "CASSANDRA_HOST not set" unless $ENV{CASSANDRA_HOST};
plan tests => 5;

{
    my $cv= AnyEvent->condvar;

    my $client= Cassandra::Client->new( contact_points => [split /,/, $ENV{CASSANDRA_HOST}], username => $ENV{CASSANDRA_USER}, password => $ENV{CASSANDRA_AUTH}, anyevent => 1);
    $client->async_connect->then(sub {
        $client->async_shutdown

    })->then(sub {
        $cv->send;
        ok(1);
    }, sub {
        $cv->send;
        ok(0) or diag($_[0]);
    });

    $cv->recv;
}



{
    my $client= Cassandra::Client->new( contact_points => [split /,/, $ENV{CASSANDRA_HOST}], username => $ENV{CASSANDRA_USER}, password => $ENV{CASSANDRA_AUTH}, anyevent => (rand()<.5) );
    eval {
        $client->connect;
        $client->shutdown;
        ok(1);
        1;
    } or do {
        ok(0) or diag($@);
    };
}

{
    my $client= Cassandra::Client->new( contact_points => [split /,/, $ENV{CASSANDRA_HOST}], username => $ENV{CASSANDRA_USER}, password => $ENV{CASSANDRA_AUTH}, anyevent => (rand()<.5) );
    my ($error)= $client->call_connect;
    ok(!$error) or diag($error);

    ($error)= $client->call_shutdown;
    ok(!$error) or diag($error);
}

{
    my $client= Cassandra::Client->new( contact_points => [split /,/, $ENV{CASSANDRA_HOST}], username => $ENV{CASSANDRA_USER}, password => $ENV{CASSANDRA_AUTH}, anyevent => (rand()<.5) );
    eval {
        my $cfuture= $client->future_connect;
        $cfuture->();

        my $sfuture= $client->future_shutdown;
        $sfuture->();

        ok(1);
    } or do {
        ok(0) or diag($@);
    };
}
