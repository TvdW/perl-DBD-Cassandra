package Cassandra::Client::Error;
use 5.008;
use strict;
use warnings;

sub new { bless { code => $_[1], message => $_[2] }, $_[0] }
use overload '""' => sub { "Error $_[0]{code}: $_[0]{message}" };
sub code { $_[0]{code} }
sub message { $_[0]{message} }

1;
