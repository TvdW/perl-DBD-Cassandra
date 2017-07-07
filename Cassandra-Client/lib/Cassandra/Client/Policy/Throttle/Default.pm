package Cassandra::Client::Policy::Throttle::Default;

use 5.010;
use strict;
use warnings;

sub new {
    my ($class, %args)= @_;
    return bless {}, $class;
}

sub should_fail {
    return 0;
}

sub count {
    return;
}

1;
