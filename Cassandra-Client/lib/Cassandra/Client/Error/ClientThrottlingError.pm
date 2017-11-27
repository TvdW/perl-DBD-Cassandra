package Cassandra::Client::Error::ClientThrottlingError;

use parent 'Cassandra::Client::Error::Base';
use 5.010;
use strict;
use warnings;

sub to_string { "Client-induced failure by throttling mechanism" }

1;
