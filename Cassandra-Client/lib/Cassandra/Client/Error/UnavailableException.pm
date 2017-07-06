package Cassandra::Client::Error::UnavailableException;

use parent 'Cassandra::Client::Error::Base';
use 5.010;
use strict;
use warnings;

sub cl { $_[0]{cl} }
sub required { $_[0]{required} }
sub alive { $_[0]{alive} }

1;
